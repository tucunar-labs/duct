package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"version"}, &out, &errb); code != 0 || strings.TrimSpace(out.String()) != "dev" {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errb.String())
	}
}

func TestModules(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"modules"}, &out, &errb); code != 0 || !strings.Contains(out.String(), "alert.channel\twebhook") {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
}

func TestUnknown(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"nope"}, &out, &errb); code != 2 {
		t.Fatalf("code=%d", code)
	}
}
