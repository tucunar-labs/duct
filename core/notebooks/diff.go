package notebooks

import (
	"fmt"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// maxDiffCells bounds the quadratic part of the diff; beyond it the changed
// middle is shown as one block removed and one added (still correct, just coarse).
const maxDiffCells = 4_000_000

// splitLines splits text into lines without their terminators ("" is no lines).
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// diffLines is a line diff of a into b: common prefix and suffix are trimmed,
// the middle goes through a longest-common-subsequence table.
func diffLines(a, b []string) []contract.DiffLine {
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a)-pre && suf < len(b)-pre && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	ma, mb := a[pre:len(a)-suf], b[pre:len(b)-suf]
	out := make([]contract.DiffLine, 0, len(a)+len(b))
	for _, l := range a[:pre] {
		out = append(out, contract.DiffLine{Op: ' ', Text: l})
	}
	switch {
	case len(ma) == 0 || len(mb) == 0 || len(ma)*len(mb) > maxDiffCells:
		for _, l := range ma {
			out = append(out, contract.DiffLine{Op: '-', Text: l})
		}
		for _, l := range mb {
			out = append(out, contract.DiffLine{Op: '+', Text: l})
		}
	default:
		out = append(out, lcsDiff(ma, mb)...)
	}
	for _, l := range a[len(a)-suf:] {
		out = append(out, contract.DiffLine{Op: ' ', Text: l})
	}
	return out
}

func lcsDiff(a, b []string) []contract.DiffLine {
	n, m := len(a), len(b)
	// t[i][j] = LCS length of a[i:], b[j:]
	t := make([][]int32, n+1)
	for i := range t {
		t[i] = make([]int32, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				t[i][j] = t[i+1][j+1] + 1
			} else {
				t[i][j] = max(t[i+1][j], t[i][j+1])
			}
		}
	}
	var out []contract.DiffLine
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			out = append(out, contract.DiffLine{Op: ' ', Text: a[i]})
			i++
			j++
		case t[i+1][j] >= t[i][j+1]:
			out = append(out, contract.DiffLine{Op: '-', Text: a[i]})
			i++
		default:
			out = append(out, contract.DiffLine{Op: '+', Text: b[j]})
			j++
		}
	}
	for ; i < n; i++ {
		out = append(out, contract.DiffLine{Op: '-', Text: a[i]})
	}
	for ; j < m; j++ {
		out = append(out, contract.DiffLine{Op: '+', Text: b[j]})
	}
	return out
}

// countChanges is how many lines a diff adds and removes.
func countChanges(d []contract.DiffLine) (added, removed int) {
	for _, l := range d {
		switch l.Op {
		case '+':
			added++
		case '-':
			removed++
		}
	}
	return added, removed
}

// withContext keeps `ctx` unchanged lines around every change and folds the
// rest into a single '@' line ("… N líneas sin cambios").
func withContext(d []contract.DiffLine, ctx int) []contract.DiffLine {
	keep := make([]bool, len(d))
	for i, l := range d {
		if l.Op == ' ' {
			continue
		}
		for k := max(0, i-ctx); k <= min(len(d)-1, i+ctx); k++ {
			keep[k] = true
		}
	}
	out := make([]contract.DiffLine, 0, len(d))
	skipped := 0
	flush := func() {
		if skipped > 0 {
			out = append(out, contract.DiffLine{Op: '@', Text: fmt.Sprintf("… %d líneas sin cambios", skipped)})
			skipped = 0
		}
	}
	for i, l := range d {
		if keep[i] {
			flush()
			out = append(out, l)
		} else {
			skipped++
		}
	}
	flush()
	return out
}
