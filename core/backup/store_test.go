package backup

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestSafeKey(t *testing.T) {
	for _, bad := range []string{"", "/etc/passwd", "../x", "a/../b", "a//b", "a/./b", `a\b`, "a/"} {
		if safeKey(bad) == nil {
			t.Errorf("safeKey(%q) accepted", bad)
		}
	}
	for _, ok := range []string{"a", "data/bronze/t/ducklake-1.parquet", "project/duct.toml"} {
		if err := safeKey(ok); err != nil {
			t.Errorf("safeKey(%q): %v", ok, err)
		}
	}
}

func TestParseManifestRejectsTraversal(t *testing.T) {
	good := `{"format":1,"id":"20261007T120000Z","files":[{"path":"data/a.parquet"}],"lake_files":[{"path":"a/b.parquet"}]}`
	if _, err := parseManifest([]byte(good)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		`{"format":2,"id":"20261007T120000Z"}`,
		`{"format":1,"id":"../x"}`,
		`{"format":1,"id":"20261007T120000Z","files":[{"path":"../../etc/passwd"}]}`,
		`{"format":1,"id":"20261007T120000Z","lake_files":[{"path":"/abs"}]}`,
	} {
		if _, err := parseManifest([]byte(bad)); err == nil {
			t.Errorf("manifest accepted: %s", bad)
		}
	}
}

func TestDirStoreRoundTripAndPrune(t *testing.T) {
	ctx := context.Background()
	s := NewDirStore(t.TempDir())
	n, sha, err := s.Put(ctx, "20261001T000000Z/data/x/y.bin", strings.NewReader("hello"))
	if err != nil || n != 5 || len(sha) != 64 {
		t.Fatalf("put: %d %q %v", n, sha, err)
	}
	rc, size, err := s.Get(ctx, "20261001T000000Z/data/x/y.bin")
	if err != nil || size != 5 {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(b) != "hello" {
		t.Errorf("got %q", b)
	}
	if _, _, err := s.Put(ctx, "../escape", strings.NewReader("x")); err == nil {
		t.Error("put outside the store")
	}

	// Three complete backups and one stale incomplete: keep 2 removes the oldest
	// complete one and the stale partial; a fresh partial stays (might be running).
	for _, id := range []string{"20261001T000000Z", "20261002T000000Z", "20261003T000000Z"} {
		if _, _, err := s.Put(ctx, id+"/"+ManifestFile, strings.NewReader("{}")); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.Put(ctx, "20250101T000000Z/catalog/t.parquet", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Put(ctx, "20261007T115900Z/catalog/t.parquet", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	if err := prune(ctx, s, 2, now, func(string, ...any) {}); err != nil {
		t.Fatal(err)
	}
	list, err := List(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range list {
		got = append(got, e.ID)
	}
	want := "20261002T000000Z 20261003T000000Z 20261007T115900Z"
	if strings.Join(got, " ") != want {
		t.Errorf("after prune: %v, want %s", got, want)
	}
}
