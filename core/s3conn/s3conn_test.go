package s3conn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestScope(t *testing.T) {
	for in, want := range map[string]string{
		"s3://lake":         "s3://lake/",
		"s3://lake/":        "s3://lake/",
		"s3://lake/a/b":     "s3://lake/a/b/",
		"s3://lake/a/b/":    "s3://lake/a/b/",
		"s3://lake//a/b///": "s3://lake/a/b/",
	} {
		got, err := Scope(in)
		if err != nil || got != want {
			t.Errorf("Scope(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"gs://x/y", "s3:///x", "./lake", "s3://"} {
		if _, err := Scope(bad); err == nil {
			t.Errorf("Scope(%q) accepted", bad)
		}
	}
}

func TestSecretSQLAndRedact(t *testing.T) {
	c := Config{Endpoint: "h:9000", URLStyle: "path", KeyID: "AK", Secret: "s'ecret", SessionToken: "tok"}
	sql := c.SecretSQL(c.Static())
	for _, want := range []string{"TEMPORARY SECRET duct_s3", "TYPE s3", "KEY_ID 'AK'", "SECRET 's''ecret'", "SESSION_TOKEN 'tok'",
		"ENDPOINT 'h:9000'", "URL_STYLE 'path'", "USE_SSL false", "REGION 'us-east-1'"} {
		if !strings.Contains(sql, want) {
			t.Errorf("secret SQL lacks %q: %s", want, sql)
		}
	}
	if got := c.Redact("boom s'ecret tok"); strings.Contains(got, "s'ecret") || strings.Contains(got, "tok") {
		t.Errorf("Redact left a secret: %q", got)
	}
	if err := (Config{Endpoint: "http://h"}).Validate(); err == nil {
		t.Error("endpoint with scheme accepted")
	}
	if err := (Config{URLStyle: "x"}).Validate(); err == nil {
		t.Error("bad url_style accepted")
	}
}

func TestSessionPolicy(t *testing.T) {
	p, err := NewProvider(Config{KeyID: "a", Secret: "b"}, "s3://lake/proj", true)
	if err != nil {
		t.Fatal(err)
	}
	pol := p.SessionPolicy()
	if !strings.Contains(pol, "arn:aws:s3:::lake/proj/*") || strings.Contains(pol, "PutObject") {
		t.Errorf("read-only policy: %s", pol)
	}
	rw, _ := NewProvider(Config{KeyID: "a", Secret: "b"}, "s3://lake/proj", false)
	if !strings.Contains(rw.SessionPolicy(), "PutObject") {
		t.Errorf("read-write policy lacks PutObject")
	}
	if _, err := NewProvider(Config{}, "s3://lake/proj", false); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Errorf("missing credentials: %v", err)
	}
}

// fakeSTS answers AssumeRole with credentials that expire after ttl and counts calls.
func fakeSTS(t *testing.T, ttl time.Duration, calls *atomic.Int32, policies *[]string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(b))
		*policies = append(*policies, form.Get("Policy"))
		n := calls.Add(1)
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials>
<AccessKeyId>ASIA%d</AccessKeyId><SecretAccessKey>sec%d</SecretAccessKey><SessionToken>tok%d</SessionToken>
<Expiration>%s</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>`, n, n, n, time.Now().Add(ttl).UTC().Format(time.RFC3339))
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestProviderRenewsBeforeExpiry(t *testing.T) {
	var calls atomic.Int32
	var pols []string
	// Lifetime 3s, credentials valid 3s: they are renewed once less than 1s is left.
	ep := fakeSTS(t, 3*time.Second, &calls, &pols)
	p, err := NewProvider(Config{Endpoint: ep, KeyID: "root", Secret: "rootsecret", RoleARN: "arn:aws:iam::x:role/y"}, "s3://lake/proj", false)
	if err != nil {
		t.Fatal(err)
	}
	p.Lifetime = 3 * time.Second
	ctx := context.Background()
	c1, err := p.Credentials(ctx)
	if err != nil || c1.SessionToken != "tok1" {
		t.Fatalf("first: %+v %v", c1, err)
	}
	if c2, _ := p.Credentials(ctx); c2.SessionToken != "tok1" || calls.Load() != 1 {
		t.Fatalf("cached credentials were refetched (%d calls)", calls.Load())
	}
	time.Sleep(2200 * time.Millisecond)
	c3, err := p.Credentials(ctx)
	if err != nil || c3.SessionToken != "tok2" {
		t.Fatalf("not renewed near expiry: %+v %v", c3, err)
	}
	if !strings.Contains(pols[0], "arn:aws:s3:::lake/proj/*") {
		t.Errorf("session policy not sent: %q", pols[0])
	}
}
