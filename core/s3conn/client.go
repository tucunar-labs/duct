package s3conn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Client is a minimal S3 client (SigV4 over net/http) for the few operations
// Duct needs outside DuckDB: backup and restore, readiness probes. It uses the
// static credentials of the Config (never the STS prefix-scoped ones), no
// multipart (objects up to 5 GiB) and no retries beyond the caller's.
type Client struct {
	cfg    Config
	http   *http.Client
	signer *v4.Signer
}

// NewClient returns a Client for cfg, which must carry credentials.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.HasCredentials() {
		return nil, errors.New("s3conn: no credentials (set DUCT_S3_ACCESS_KEY_ID and DUCT_S3_SECRET_ACCESS_KEY, or AWS_*)")
	}
	return &Client{
		cfg:    cfg,
		http:   &http.Client{Timeout: 0},
		signer: v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }),
	}, nil
}

// Config returns the connection settings (with their secrets: do not log).
func (c *Client) Config() Config { return c.cfg }

// SameServer reports whether other points at the same endpoint, so an object
// can be copied between their buckets without leaving the server.
func (c *Client) SameServer(other *Client) bool {
	return other != nil && c.cfg.Endpoint == other.cfg.Endpoint && c.cfg.Region == other.cfg.Region &&
		c.cfg.UseSSL == other.cfg.UseSSL && c.cfg.KeyID == other.cfg.KeyID
}

func (c *Client) region() string {
	if c.cfg.Region != "" {
		return c.cfg.Region
	}
	return "us-east-1"
}

// Object describes one stored object.
type Object struct {
	Key  string
	Size int64
	ETag string
}

// StatusError is a non-2xx answer. Its message never carries credentials.
type StatusError struct {
	Op     string
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	b := strings.TrimSpace(e.Body)
	if len(b) > 300 {
		b = b[:300]
	}
	return fmt.Sprintf("s3 %s: HTTP %d %s", e.Op, e.Status, b)
}

// IsNotFound reports whether err is a 404 from the store.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Status == http.StatusNotFound
}

func (c *Client) endpointURL(bucket, key string, query url.Values) *url.URL {
	scheme := "http"
	if c.cfg.UseSSL {
		scheme = "https"
	}
	host := c.cfg.Endpoint
	style := c.cfg.URLStyle
	if host == "" {
		host = "s3." + c.region() + ".amazonaws.com"
		if style == "" {
			style = "vhost"
		}
	}
	u := &url.URL{Scheme: scheme, Host: host}
	if style == "vhost" {
		u.Host = bucket + "." + host
		u.Path = "/" + key
	} else {
		u.Path = "/" + bucket + "/" + key
	}
	if key == "" && style != "vhost" {
		u.Path = "/" + bucket
	}
	u.RawPath = escapePath(u.Path)
	u.RawQuery = query.Encode()
	return u
}

// escapePath percent-encodes everything but unreserved characters and "/".
func escapePath(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		ch := p[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9', ch == '-', ch == '_', ch == '.', ch == '~', ch == '/':
			b.WriteByte(ch)
		default:
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

func (c *Client) do(ctx context.Context, op, method string, u *url.URL, body io.Reader, size int64, payloadHash string, hdr map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if size >= 0 && body != nil {
		req.ContentLength = size
	}
	if body == nil || size == 0 {
		req.Body = http.NoBody
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	err = c.signer.SignHTTP(ctx, aws.Credentials{AccessKeyID: c.cfg.KeyID, SecretAccessKey: c.cfg.Secret, SessionToken: c.cfg.SessionToken},
		req, payloadHash, "s3", c.region(), time.Now())
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.redactErr(op, err)
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, &StatusError{Op: op, Status: resp.StatusCode, Body: string(b)}
	}
	return resp, nil
}

func (c *Client) redactErr(op string, err error) error {
	return fmt.Errorf("s3 %s: %s", op, c.cfg.Redact(err.Error()))
}

var emptyHash = hex.EncodeToString(func() []byte { h := sha256.Sum256(nil); return h[:] }())

// Head returns the object, or a StatusError with Status 404.
func (c *Client) Head(ctx context.Context, bucket, key string) (Object, error) {
	resp, err := c.do(ctx, "head "+key, http.MethodHead, c.endpointURL(bucket, key, nil), nil, 0, emptyHash, nil)
	if err != nil {
		return Object{}, err
	}
	_ = resp.Body.Close()
	return Object{Key: key, Size: resp.ContentLength, ETag: strings.Trim(resp.Header.Get("ETag"), `"`)}, nil
}

// Get opens an object for reading; the caller closes it. size is its length.
func (c *Client) Get(ctx context.Context, bucket, key string) (body io.ReadCloser, size int64, err error) {
	resp, err := c.do(ctx, "get "+key, http.MethodGet, c.endpointURL(bucket, key, nil), nil, 0, emptyHash, nil)
	if err != nil {
		return nil, 0, err
	}
	return resp.Body, resp.ContentLength, nil
}

// Put stores size bytes from body. sha256hex is the hex SHA-256 of the whole
// payload (required: it is signed).
func (c *Client) Put(ctx context.Context, bucket, key string, body io.Reader, size int64, sha256hex string) error {
	resp, err := c.do(ctx, "put "+key, http.MethodPut, c.endpointURL(bucket, key, nil), body, size, sha256hex, nil)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// PutBytes stores a small payload.
func (c *Client) PutBytes(ctx context.Context, bucket, key string, data []byte) error {
	sum := sha256.Sum256(data)
	return c.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), hex.EncodeToString(sum[:]))
}

// Delete removes an object (no error if it does not exist).
func (c *Client) Delete(ctx context.Context, bucket, key string) error {
	resp, err := c.do(ctx, "delete "+key, http.MethodDelete, c.endpointURL(bucket, key, nil), nil, 0, emptyHash, nil)
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return err
	}
	return resp.Body.Close()
}

// Copy copies an object inside the server (source and destination buckets may
// differ, objects up to 5 GiB).
func (c *Client) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	src := "/" + srcBucket + "/" + escapePath(srcKey)
	resp, err := c.do(ctx, "copy "+srcKey, http.MethodPut, c.endpointURL(dstBucket, dstKey, nil), nil, 0, emptyHash,
		map[string]string{"X-Amz-Copy-Source": src})
	if err != nil {
		return err
	}
	// CopyObject can answer 200 with an error document.
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if bytes.Contains(b, []byte("<Error>")) {
		return &StatusError{Op: "copy " + srcKey, Status: resp.StatusCode, Body: string(b)}
	}
	return nil
}

type listResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
		ETag string `xml:"ETag"`
	} `xml:"Contents"`
}

// List returns every object whose key starts with prefix, following
// continuation tokens.
func (c *Client) List(ctx context.Context, bucket, prefix string) ([]Object, error) {
	var out []Object
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := c.do(ctx, "list "+prefix, http.MethodGet, c.endpointURL(bucket, "", q), nil, 0, emptyHash, nil)
		if err != nil {
			return nil, err
		}
		var r listResult
		err = xml.NewDecoder(resp.Body).Decode(&r)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, o := range r.Contents {
			out = append(out, Object{Key: o.Key, Size: o.Size, ETag: strings.Trim(o.ETag, `"`)})
		}
		if !r.IsTruncated || r.NextContinuationToken == "" {
			return out, nil
		}
		token = r.NextContinuationToken
	}
}
