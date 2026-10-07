// Package s3conn describes how Duct reaches an S3-compatible object store
// (RustFS, Garage, AWS S3) for `storage = "s3://bucket/prefix"` (ADR 0001):
// endpoint, region, URL style and credentials, the DuckDB temporary secret that
// carries them, and optional STS AssumeRole credentials scoped to the lake
// prefix by a session policy (ADR 0007).
//
// Credentials are secrets: they come from the environment (never duct.toml),
// are only ever written into CREATE TEMPORARY SECRET statements (persistent
// secrets are disabled), and Redact removes them from text that may echo them.
package s3conn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Environment variables (the AWS names are the fallback of the DUCT_S3_ ones).
const (
	EnvEndpoint     = "DUCT_S3_ENDPOINT"
	EnvRegion       = "DUCT_S3_REGION"
	EnvURLStyle     = "DUCT_S3_URL_STYLE"
	EnvUseSSL       = "DUCT_S3_USE_SSL"
	EnvRoleARN      = "DUCT_S3_ROLE_ARN"
	EnvKeyID        = "DUCT_S3_ACCESS_KEY_ID"
	EnvSecret       = "DUCT_S3_SECRET_ACCESS_KEY" //nolint:gosec // variable name, not a secret
	EnvSessionToken = "DUCT_S3_SESSION_TOKEN"
)

// SecretName is the DuckDB temporary secret that holds the S3 credentials.
const SecretName = "duct_s3"

// Config is the connection to the object store. The zero value is not usable:
// a Config always comes from the project (core/project) or a test.
type Config struct {
	Endpoint string // host[:port], no scheme; empty = AWS S3
	Region   string // default us-east-1
	URLStyle string // "path" (default; RustFS, Garage) or "vhost"
	UseSSL   bool
	// RoleARN, if set, makes the process trade its credentials for short-lived
	// STS credentials limited to the lake prefix (see Provider).
	RoleARN string

	KeyID, Secret, SessionToken string // secrets
}

// Scheme is the URL scheme of a remote storage setting.
func Scheme(storage string) string {
	if i := strings.Index(storage, "://"); i > 0 {
		return storage[:i]
	}
	return ""
}

// ParseStorage splits s3://bucket/prefix. The prefix has no leading or trailing
// slash and may be empty.
func ParseStorage(storage string) (bucket, prefix string, err error) {
	u, err := url.Parse(storage)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return "", "", fmt.Errorf("s3conn: storage must look like s3://bucket/prefix, got %q", storage)
	}
	return u.Host, strings.Trim(u.Path, "/"), nil
}

// Scope returns the allowed_directories entry for storage: s3://bucket/prefix/.
func Scope(storage string) (string, error) {
	b, p, err := ParseStorage(storage)
	if err != nil {
		return "", err
	}
	if p == "" {
		return "s3://" + b + "/", nil
	}
	return "s3://" + b + "/" + p + "/", nil
}

// Validate checks the fields that do not depend on the credentials.
func (c Config) Validate() error {
	switch c.URLStyle {
	case "", "path", "vhost":
	default:
		return fmt.Errorf("s3conn: url_style must be \"path\" or \"vhost\", got %q", c.URLStyle)
	}
	if strings.Contains(c.Endpoint, "://") || strings.Contains(c.Endpoint, "/") {
		return errors.New("s3conn: endpoint is host[:port] without scheme (use use_ssl to pick http/https)")
	}
	return nil
}

// HasCredentials reports whether a key pair is present.
func (c Config) HasCredentials() bool { return c.KeyID != "" && c.Secret != "" }

func q(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// SecretSQL is CREATE OR REPLACE TEMPORARY SECRET with the given credentials
// (the static ones, or those of a Provider). It carries secrets: never log it.
func (c Config) SecretSQL(cr Credentials) string {
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	style := c.URLStyle
	if style == "" {
		style = "path"
	}
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TEMPORARY SECRET " + SecretName + " (TYPE s3")
	b.WriteString(", KEY_ID " + q(cr.KeyID) + ", SECRET " + q(cr.Secret))
	if cr.SessionToken != "" {
		b.WriteString(", SESSION_TOKEN " + q(cr.SessionToken))
	}
	b.WriteString(", REGION " + q(region) + ", URL_STYLE " + q(style))
	if c.Endpoint != "" {
		b.WriteString(", ENDPOINT " + q(c.Endpoint))
	}
	fmt.Fprintf(&b, ", USE_SSL %t)", c.UseSSL)
	return b.String()
}

// Redact removes every credential value from text.
func (c Config) Redact(text string) string {
	for _, s := range []string{c.Secret, c.SessionToken} {
		if s != "" {
			text = strings.ReplaceAll(text, s, "***")
		}
	}
	return text
}

// Credentials is a key pair, with the session token and expiry of STS ones.
type Credentials struct {
	KeyID, Secret, SessionToken string
	Expires                     time.Time // zero = does not expire
}

// Static returns the credentials of the Config itself.
func (c Config) Static() Credentials {
	return Credentials{KeyID: c.KeyID, Secret: c.Secret, SessionToken: c.SessionToken}
}

// Provider hands out credentials for one lake prefix. Without RoleARN they are
// the static ones. With RoleARN it calls STS AssumeRole with a session policy
// that allows only the lake prefix (read only when ReadOnly), caches the result
// and renews it when a third of its lifetime is left.
type Provider struct {
	cfg      Config
	bucket   string
	prefix   string
	readOnly bool
	// Lifetime of the STS credentials (default 1h; RustFS minimum is 15m).
	Lifetime time.Duration

	mu  sync.Mutex
	cur Credentials
}

// NewProvider validates cfg and storage. It does not call the network.
func NewProvider(cfg Config, storage string, readOnly bool) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b, p, err := ParseStorage(storage)
	if err != nil {
		return nil, err
	}
	if !cfg.HasCredentials() {
		return nil, fmt.Errorf("s3conn: no credentials: set %s and %s (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)", EnvKeyID, EnvSecret)
	}
	return &Provider{cfg: cfg, bucket: b, prefix: p, readOnly: readOnly, Lifetime: time.Hour}, nil
}

// Config is the connection the provider was built with.
func (p *Provider) Config() Config { return p.cfg }

// Temporary reports whether credentials expire (STS in use).
func (p *Provider) Temporary() bool { return p.cfg.RoleARN != "" }

// Credentials returns valid credentials, renewing them through STS when needed.
// Errors never include secrets.
func (p *Provider) Credentials(ctx context.Context) (Credentials, error) {
	if !p.Temporary() {
		return p.cfg.Static(), nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur.KeyID != "" && time.Until(p.cur.Expires) > p.Lifetime/3 {
		return p.cur, nil
	}
	cr, err := p.assume(ctx)
	if err != nil {
		return Credentials{}, err
	}
	p.cur = cr
	return cr, nil
}

// SessionPolicy is the IAM policy (JSON) that limits STS credentials to the
// lake prefix. It intersects with the policy of the user that assumes the role.
func (p *Provider) SessionPolicy() string {
	objects := "arn:aws:s3:::" + p.bucket + "/*"
	list := map[string]any{}
	if p.prefix != "" {
		objects = "arn:aws:s3:::" + p.bucket + "/" + p.prefix + "/*"
		list = map[string]any{"StringLike": map[string]any{"s3:prefix": []string{p.prefix, p.prefix + "/*"}}}
	}
	actions := []string{"s3:GetObject"}
	if !p.readOnly {
		actions = append(actions, "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts")
	}
	listStmt := map[string]any{
		"Effect": "Allow", "Action": []string{"s3:ListBucket", "s3:GetBucketLocation"},
		"Resource": "arn:aws:s3:::" + p.bucket,
	}
	if len(list) > 0 {
		listStmt["Condition"] = list
	}
	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{"Effect": "Allow", "Action": actions, "Resource": objects},
			listStmt,
		},
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

func (p *Provider) assume(ctx context.Context) (Credentials, error) {
	scheme := "https"
	if !p.cfg.UseSSL {
		scheme = "http"
	}
	region := p.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := sts.Options{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(p.cfg.KeyID, p.cfg.Secret, p.cfg.SessionToken),
	}
	if p.cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(scheme + "://" + p.cfg.Endpoint)
	}
	secs := int32(p.Lifetime.Seconds())
	out, err := sts.New(opts).AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(p.cfg.RoleARN),
		RoleSessionName: aws.String("duct"),
		Policy:          aws.String(p.SessionPolicy()),
		DurationSeconds: aws.Int32(secs),
	})
	if err != nil {
		return Credentials{}, errors.New("s3conn: STS AssumeRole failed: " + p.cfg.Redact(err.Error()))
	}
	if out.Credentials == nil {
		return Credentials{}, errors.New("s3conn: STS AssumeRole returned no credentials")
	}
	c := out.Credentials
	cr := Credentials{KeyID: aws.ToString(c.AccessKeyId), Secret: aws.ToString(c.SecretAccessKey), SessionToken: aws.ToString(c.SessionToken)}
	if c.Expiration != nil {
		cr.Expires = *c.Expiration
	} else {
		cr.Expires = time.Now().Add(p.Lifetime)
	}
	return cr, nil
}
