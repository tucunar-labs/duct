package notebooks

import (
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

func render(d []contract.DiffLine) string {
	var b strings.Builder
	for _, l := range d {
		b.WriteByte(l.Op)
		b.WriteString(l.Text)
		b.WriteByte('\n')
	}
	return b.String()
}

func TestDiffLines(t *testing.T) {
	a := splitLines("a\nb\nc\nd\n")
	b := splitLines("a\nB\nc\nd\ne\n")
	d := diffLines(a, b)
	if got, want := render(d), " a\n-b\n+B\n c\n d\n+e\n"; got != want {
		t.Errorf("diff =\n%s want\n%s", got, want)
	}
	if add, rem := countChanges(d); add != 2 || rem != 1 {
		t.Errorf("counts = +%d -%d", add, rem)
	}
	if d := diffLines(nil, splitLines("x\ny\n")); render(d) != "+x\n+y\n" {
		t.Errorf("from nothing: %q", render(d))
	}
	if d := diffLines(splitLines("x\n"), nil); render(d) != "-x\n" {
		t.Errorf("to nothing: %q", render(d))
	}
	if d := diffLines(a, a); len(d) != 4 {
		t.Errorf("identical: %q", render(d))
	} else if add, rem := countChanges(d); add+rem != 0 {
		t.Errorf("identical counts: %d %d", add, rem)
	}
}

// A huge change falls back to a coarse (still correct) diff instead of a quadratic table.
func TestDiffLinesHuge(t *testing.T) {
	var a, b []string
	for i := range 3000 {
		a = append(a, "a"+strings.Repeat("x", i%7))
		b = append(b, "b"+strings.Repeat("y", i%5))
	}
	add, rem := countChanges(diffLines(a, b))
	if add != 3000 || rem != 3000 {
		t.Errorf("huge: +%d -%d", add, rem)
	}
}

func TestWithContext(t *testing.T) {
	var lines []string
	for i := range 30 {
		lines = append(lines, string(rune('a'+i%26))+strings.Repeat("_", i))
	}
	changed := append([]string{}, lines...)
	changed[15] = "CHANGED"
	out := withContext(diffLines(lines, changed), 2)
	got := render(out)
	if !strings.Contains(got, "@… 13 líneas sin cambios\n") || !strings.Contains(got, "-p_______________\n+CHANGED\n") || strings.Count(got, "@") != 2 {
		t.Errorf("folded diff:\n%s", got)
	}
}

func TestValidName(t *testing.T) {
	for _, ok := range []string{"a", "ventas_2026", "A-b", strings.Repeat("a", 64)} {
		if !ValidName(ok) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"", "_x", ".x", "../x", "a/b", "a b", "a.py", strings.Repeat("a", 65), "ñ"} {
		if ValidName(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}
