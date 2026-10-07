package pipeline

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

func TestParseGolden(t *testing.T) {
	files, _ := filepath.Glob("testdata/parse/*.sql")
	if len(files) < 3 {
		t.Fatalf("golden inputs missing: %v", files)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			tables, err := Parse("x.sql", string(src))
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.MarshalIndent(tables, "", "  ")
			got = append(got, '\n')
			golden := strings.TrimSuffix(f, ".sql") + ".golden.json"
			if *update {
				if err := os.WriteFile(golden, got, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatal(err)
			}
			if string(want) != string(got) {
				t.Errorf("golden mismatch for %s\n--- got\n%s\n--- want\n%s", f, got, want)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct{ name, src, want string }{
		{"not create", "SELECT 1", "1:1: unsupported statement"},
		{"create table", "CREATE TABLE a AS SELECT 1", "expected MATERIALIZED VIEW or STREAMING TABLE"},
		{"no as", "CREATE MATERIALIZED VIEW a SELECT 1", "expected AS"},
		{"no select", "CREATE MATERIALIZED VIEW a AS ;", "missing SELECT"},
		{"empty body", "CREATE MATERIALIZED VIEW a AS", "missing SELECT"},
		{"catalog name", "CREATE MATERIALIZED VIEW a.b.c AS SELECT 1", "no catalog"},
		{"unterminated string", "CREATE MATERIALIZED VIEW a AS SELECT 'x", "unterminated string starting at 1:"},
		{"unterminated comment", "SELECT 1 /* x", "unterminated /* comment"},
		{"dup constraint", "CREATE MATERIALIZED VIEW a (CONSTRAINT c EXPECT (1=1), CONSTRAINT C EXPECT (2=2)) AS SELECT 1", `duplicate constraint "C"`},
		{"empty expr", "CREATE MATERIALIZED VIEW a (CONSTRAINT c EXPECT ( )) AS SELECT 1", "empty expression"},
		{"bad violation", "CREATE MATERIALIZED VIEW a (CONSTRAINT c EXPECT (x) ON VIOLATION EXPLODE) AS SELECT 1", "expected DROP ROW or FAIL UPDATE"},
		{"unbalanced", "CREATE MATERIALIZED VIEW a (CONSTRAINT c EXPECT (x > (1) AS SELECT 1", "unbalanced parenthesis"},
		{"missing comma", "CREATE MATERIALIZED VIEW a (CONSTRAINT c EXPECT (x) CONSTRAINT d EXPECT (y)) AS SELECT 1", "expected , or )"},
		{"column list", "CREATE MATERIALIZED VIEW a (id INTEGER) AS SELECT 1", "expected CONSTRAINT"},
		{"line number", "\n\n  DROP TABLE x", "x.sql:3:3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse("x.sql", c.src)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want containing %q", err, c.want)
			}
		})
	}
}

func TestParseDetails(t *testing.T) {
	ts, err := Parse("f.sql", "CREATE MATERIALIZED VIEW T AS SELECT 1;\n\nCREATE STREAMING TABLE s.U AS SELECT 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 2 || ts[0].ID() != "main.t" || ts[1].ID() != "s.u" || !ts[1].Streaming || ts[1].Line != 3 {
		t.Fatalf("%+v", ts)
	}
	if ts[0].Query != "SELECT 1" {
		t.Errorf("query = %q", ts[0].Query)
	}
	// Whitespace-only and comment-only files are fine and empty.
	if ts, err := Parse("e.sql", "-- nothing\n;;"); err != nil || len(ts) != 0 {
		t.Errorf("empty: %v %v", ts, err)
	}
}
