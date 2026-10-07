package sharing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/alonsoburon/duct/core/s3conn"
)

// S3Storage keeps the copies in an S3-compatible bucket (RustFS, Garage, AWS)
// and hands out presigned GET URLs.
type S3Storage struct {
	bucket, prefix string
	api            *s3.Client
	presign        *s3.PresignClient
}

var _ Storage = (*S3Storage)(nil)

// S3Options tune NewS3Storage.
type S3Options struct {
	// PublicEndpoint (host[:port]) is the address recipients reach the store
	// at, when it differs from the one Duct uses; empty = the same.
	PublicEndpoint string
	// PublicUseSSL applies to PublicEndpoint (default: the connection's).
	PublicUseSSL *bool
}

// NewS3Storage opens storage = s3://bucket/prefix with the credentials of prov
// (read and write on that prefix; STS ones are renewed on demand).
func NewS3Storage(storage string, prov *s3conn.Provider, opt S3Options) (*S3Storage, error) {
	bucket, prefix, err := s3conn.ParseStorage(storage)
	if err != nil {
		return nil, err
	}
	cfg := prov.Config()
	creds := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		c, err := prov.Credentials(ctx)
		if err != nil {
			return aws.Credentials{}, err
		}
		return aws.Credentials{AccessKeyID: c.KeyID, SecretAccessKey: c.Secret, SessionToken: c.SessionToken, Source: "duct", CanExpire: !c.Expires.IsZero(), Expires: c.Expires}, nil
	})
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	mk := func(endpoint string, ssl bool) *s3.Client {
		o := s3.Options{
			Region:                     region,
			Credentials:                creds,
			UsePathStyle:               cfg.URLStyle != "vhost",
			RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
			ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		}
		if endpoint != "" {
			scheme := "https"
			if !ssl {
				scheme = "http"
			}
			o.BaseEndpoint = aws.String(scheme + "://" + endpoint)
		}
		return s3.New(o)
	}
	api := mk(cfg.Endpoint, cfg.UseSSL)
	pub := api
	if opt.PublicEndpoint != "" {
		ssl := cfg.UseSSL
		if opt.PublicUseSSL != nil {
			ssl = *opt.PublicUseSSL
		}
		pub = mk(opt.PublicEndpoint, ssl)
	}
	return &S3Storage{bucket: bucket, prefix: prefix, api: api, presign: s3.NewPresignClient(pub)}, nil
}

func (s *S3Storage) full(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// Put implements Storage. src must be an io.ReadSeeker or a size must be known;
// the bytes are sent with that content length.
func (s *S3Storage) Put(ctx context.Context, key string, src io.Reader, size int64) error {
	if err := validKey(key); err != nil {
		return err
	}
	rs, ok := src.(io.ReadSeeker)
	if !ok {
		return errors.New("sharing: S3 Put needs a seekable source")
	}
	_, err := s.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.full(key)),
		Body: rs, ContentLength: aws.Int64(size), ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("sharing: put %s: %w", key, err)
	}
	return nil
}

// List implements Storage.
func (s *S3Storage) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	p := s3.NewListObjectsV2Paginator(s.api, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(s.full(prefix))})
	root := ""
	if s.prefix != "" {
		root = s.prefix + "/"
	}
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("sharing: list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			out = append(out, Object{Key: strings.TrimPrefix(aws.ToString(o.Key), root), Size: aws.ToInt64(o.Size), Modified: aws.ToTime(o.LastModified)})
		}
	}
	return out, nil
}

// Delete implements Storage.
func (s *S3Storage) Delete(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		n := min(len(keys), 500)
		ids := make([]types.ObjectIdentifier, n)
		for i, k := range keys[:n] {
			if err := validKey(k); err != nil {
				return err
			}
			ids[i] = types.ObjectIdentifier{Key: aws.String(s.full(k))}
		}
		if _, err := s.api.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(s.bucket), Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)}}); err != nil {
			return fmt.Errorf("sharing: delete: %w", err)
		}
		keys = keys[n:]
	}
	return nil
}

// URL implements Storage with a presigned GET.
func (s *S3Storage) URL(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	r, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.full(key))}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("sharing: presign %s: %w", key, err)
	}
	return r.URL, nil
}
