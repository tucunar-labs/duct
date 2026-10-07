package pgconn

import (
	"strings"
	"testing"
)

func TestParseForms(t *testing.T) {
	for _, in := range []string{
		"postgres://duct:p%40ss@db.example:5433/lake?sslmode=require",
		"postgres:dbname=lake host=db.example port=5433 user=duct password='p@ss' sslmode=require",
	} {
		p, err := Parse(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if p["host"] != "db.example" || p["port"] != "5433" || p["database"] != "lake" || p["user"] != "duct" || p["password"] != "p@ss" || p["sslmode"] != "require" {
			t.Errorf("%q parsed as %v", in, p)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, in := range []string{"/tmp/x.sqlite", "postgres://h", "postgres:host=h", "postgres:dbname=d bogus=1", "postgres:dbname='x"} {
		_, err := Parse(in)
		if err == nil {
			t.Errorf("%q: want error", in)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("error leaks input: %v", err)
		}
	}
}

func TestPasswordHandling(t *testing.T) {
	got, had, err := StripPassword("postgres://duct:s3cret@h:5432/lake")
	if err != nil || !had || strings.Contains(got, "s3cret") || !strings.Contains(got, "duct@h:5432/lake") {
		t.Fatalf("strip url: %q %v %v", got, had, err)
	}
	got, had, _ = StripPassword("postgres:dbname=lake host=h user=duct password=s3cret")
	if !had || strings.Contains(got, "s3cret") || !strings.HasPrefix(got, "postgres:") {
		t.Fatalf("strip kw: %q", got)
	}
	with, err := WithPassword("postgres://duct@h/lake", "a b&c")
	if err != nil {
		t.Fatal(err)
	}
	p, err := Parse(with)
	if err != nil || p["password"] != "a b&c" {
		t.Fatalf("with password: %q %v %v", with, p, err)
	}
	// An explicit password wins over the env one.
	if same, _ := WithPassword("postgres://duct:x@h/lake", "other"); !strings.Contains(same, ":x@") {
		t.Errorf("explicit password replaced: %q", same)
	}
	pp, _ := Parse("postgres://duct:s3cret-value@h/lake")
	if r := pp.Redact("failed postgres://duct:s3cret-value@h/lake pw s3cret-value host=h password=s3cret-value"); strings.Contains(r, "s3cret") {
		t.Errorf("redact: %q", r)
	}
}
