package servehttp

import "testing"

func TestSplitFormat(t *testing.T) {
	for in, want := range map[string][2]string{
		"select 1 FORMAT JSON":         {"select 1", "JSON"},
		"select 1 format csv ;":        {"select 1", "csv"},
		"select 'a FORMAT JSON'":       {"select 'a FORMAT JSON'", ""},
		"select 1;":                    {"select 1", ""},
		"select 1\nFORMAT ArrowStream": {"select 1", "ArrowStream"},
	} {
		if q, f := splitFormat(in); q != want[0] || f != want[1] {
			t.Errorf("%q -> %q, %q", in, q, f)
		}
	}
}
