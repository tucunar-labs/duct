package governance

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

func TestParseGolden(t *testing.T) {
	files, _ := filepath.Glob("testdata/sql/*.sql")
	if len(files) < 10 {
		t.Fatalf("golden inputs missing: %v", files)
	}
	for _, f := range files {
		t.Run(strings.TrimSuffix(filepath.Base(f), ".sql"), func(t *testing.T) {
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			st, err := Parse(string(src))
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.MarshalIndent(st, "", "  ")
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
		{"create role no name", "CREATE ROLE", "1:12: expected a role name, found end of statement"},
		{"grant other privilege", "GRANT INSERT ON t TO r", "1:7: only the SELECT privilege exists, found INSERT"},
		{"grant no on", "GRANT SELECT gold.t TO r", `1:14: expected ON, found "gold"`},
		{"grant wrong prep", "GRANT SELECT ON gold.t FROM r", `1:24: expected TO, found "FROM"`},
		{"revoke wrong prep", "REVOKE SELECT ON gold.t TO r", `1:25: expected FROM, found "TO"`},
		{"catalog part", "GRANT SELECT ON a.b.c TO r", "1:20: use schema.table (no catalog part)"},
		{"revoke role", "REVOKE ROLE r FROM USER u", "1:8: REVOKE ROLE is not supported"},
		{"grant role no user", "GRANT ROLE r TO u", `1:17: expected USER, found "u"`},
		{"two statements", "CREATE ROLE a; CREATE ROLE b", "1:14: only one statement per call"},
		{"trailing junk", "CREATE ROLE a b", `1:15: unexpected "b" after the end of the statement`},
		{"filter no role", "ALTER TABLE g.t SET ROW FILTER x = 1", "1:37: expected FOR ROLE <role> at the end of the row filter"},
		{"filter empty", "ALTER TABLE g.t SET ROW FILTER FOR ROLE r", "1:32: expected an expression before FOR ROLE"},
		{"filter missing", "ALTER TABLE g.t SET ROW FILTER", "1:31: expected an expression for the row filter"},
		{"mask no role", "ALTER TABLE g.t ALTER COLUMN c SET MASK 'x'", "expected FOR ROLE <role> at the end of the mask"},
		{"databricks filter", "ALTER TABLE g.t SET ROW FILTER my_fn ON (region)", "Databricks SQL-UDF row filters"},
		{"databricks mask", "ALTER TABLE g.t ALTER COLUMN c SET MASK my_fn USING COLUMNS (d)", "Databricks SQL-UDF masks"},
		{"second line", "GRANT SELECT\nON gold.t\nTO", "3:3: expected a role name, found end of statement"},
		{"unterminated string", "ALTER TABLE g.t SET ROW FILTER x = 'a FOR ROLE r", "1:36: unterminated string"},
		{"unterminated comment", "GRANT SELECT /* x", "1:14: unterminated /* comment"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(c.src)
			var pe *ParseError
			if !errors.As(err, &pe) {
				t.Fatalf("want a ParseError, got %v", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error = %q, want it to contain %q", err, c.want)
			}
		})
	}
}

func TestParseNotGovernance(t *testing.T) {
	for _, q := range []string{
		"", "   ", "SELECT 1", "CREATE TABLE a (i INT)", "DROP TABLE a", "SHOW TABLES",
		"ALTER TABLE g.t ADD COLUMN x INT", "ALTER TABLE g.t ALTER COLUMN c SET DATA TYPE INT",
		"ALTER TABLE g.t DROP COLUMN c", "CREATE ROLEX", "-- GRANT\nSELECT 1", "INSERT INTO t VALUES ('GRANT')",
	} {
		if _, err := Parse(q); !errors.Is(err, ErrNotGovernance) {
			t.Errorf("%q: err = %v, want ErrNotGovernance", q, err)
		}
	}
}

func TestExprOffset(t *testing.T) {
	src := "ALTER TABLE g.t SET ROW FILTER   a = 1  FOR ROLE r"
	st, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if st.Expr != "a = 1" || src[st.ExprPos:st.ExprPos+5] != "a = 1" {
		t.Errorf("expr %q at %d", st.Expr, st.ExprPos)
	}
}
