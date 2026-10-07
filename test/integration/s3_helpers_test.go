//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// RustFS in rootless Podman, bound to 127.0.0.1. Override the image with
// DUCT_TEST_RUSTFS_IMAGE; point the tests at an existing S3-compatible server
// instead of Podman with DUCT_TEST_S3_ENDPOINT / _ACCESS_KEY / _SECRET_KEY
// (CI services). Region is always us-east-1, URL style path, plain http.
const (
	rustfsImage  = "docker.io/rustfs/rustfs:latest"
	rustfsAccess = "ductadmin"
	rustfsSecret = "ductadmin-secret-key"
)

type s3Server struct {
	endpoint string // host:port
	access   string
	secret   string
}

// startRustFS runs RustFS, waits until it answers S3 requests and removes it at
// the end of the test.
func startRustFS(t *testing.T) *s3Server {
	t.Helper()
	var s *s3Server
	if ep := os.Getenv("DUCT_TEST_S3_ENDPOINT"); ep != "" {
		s = &s3Server{endpoint: ep, access: os.Getenv("DUCT_TEST_S3_ACCESS_KEY"), secret: os.Getenv("DUCT_TEST_S3_SECRET_KEY")}
	} else {
		if _, err := exec.LookPath("podman"); err != nil {
			t.Skip("podman not available")
		}
		port := freePort(t)
		image := rustfsImage
		if v := os.Getenv("DUCT_TEST_RUSTFS_IMAGE"); v != "" {
			image = v
		}
		id := podman(t, "run", "-d", "--rm",
			"-e", "RUSTFS_ACCESS_KEY="+rustfsAccess, "-e", "RUSTFS_SECRET_KEY="+rustfsSecret,
			"-p", fmt.Sprintf("127.0.0.1:%d:9000", port), image, "/data")
		t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", id).Run() })
		s = &s3Server{endpoint: fmt.Sprintf("127.0.0.1:%d", port), access: rustfsAccess, secret: rustfsSecret}
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		// A signed bucket listing answers 200 once the server is ready.
		if code, _ := s.do(http.MethodGet, "/", nil); code == http.StatusOK {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatal("rustfs did not start")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// env is the credential/endpoint environment for duct subprocesses.
func (s *s3Server) env() []string {
	return []string{"DUCT_S3_ACCESS_KEY_ID=" + s.access, "DUCT_S3_SECRET_ACCESS_KEY=" + s.secret,
		"DUCT_S3_ENDPOINT=" + s.endpoint, "DUCT_S3_USE_SSL=false"}
}

// do sends a SigV4-signed request with the root credentials.
func (s *s3Server) do(method, path string, body []byte) (int, string) {
	req, err := http.NewRequestWithContext(context.Background(), method, "http://"+s.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return 0, err.Error()
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	err = v4.NewSigner().SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: s.access, SecretAccessKey: s.secret}, req, hash, "s3", "us-east-1", time.Now())
	if err != nil {
		return 0, err.Error()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (s *s3Server) must(t *testing.T, method, path string, body []byte) string {
	t.Helper()
	code, out := s.do(method, path, body)
	if code/100 != 2 {
		t.Fatalf("%s %s: HTTP %d %s", method, path, code, out)
	}
	return out
}

func (s *s3Server) createBucket(t *testing.T, bucket string) {
	s.must(t, http.MethodPut, "/"+bucket, nil)
}

func (s *s3Server) put(t *testing.T, bucket, key, body string) {
	s.must(t, http.MethodPut, "/"+bucket+"/"+key, []byte(body))
}

// keys lists the object keys under prefix (ListObjectsV2, no pagination: the
// tests are small).
func (s *s3Server) keys(t *testing.T, bucket, prefix string) []string {
	t.Helper()
	out := s.must(t, http.MethodGet, "/"+bucket+"?list-type=2&prefix="+prefix, nil)
	var ks []string
	for _, part := range strings.Split(out, "<Key>")[1:] {
		ks = append(ks, part[:strings.Index(part, "</Key>")])
	}
	return ks
}
