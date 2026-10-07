package contracts

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	odcs "github.com/alonsoburon/odcs-go"
)

// FileDiff is the change of one contract file against a git ref.
type FileDiff struct {
	File    string        `json:"file"`
	Status  string        `json:"status"` // added | removed | changed | unchanged
	Changes []odcs.Change `json:"changes,omitempty"`
}

// Breaking is true for a removed contract (its table loses governance) or
// when odcs-go finds a breaking change.
func (d FileDiff) Breaking() bool { return d.Status == "removed" || odcs.HasBreaking(d.Changes) }

func git(ctx context.Context, root string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		msg := err.Error()
		if ee, ok := err.(*exec.ExitError); ok { //nolint:errorlint // direct from Output
			msg = strings.TrimSpace(string(ee.Stderr))
		}
		return nil, fmt.Errorf("git %s: %s", strings.Join(args, " "), msg)
	}
	return out, nil
}

// DiffAgainst compares the working-tree contracts with their version at ref
// (any git rev; default HEAD). New files are "added", files missing from the
// working tree are "removed".
func DiffAgainst(ctx context.Context, root, ref string) ([]FileDiff, error) {
	if ref == "" {
		ref = "HEAD"
	}
	listed, err := git(ctx, root, "ls-tree", "-r", "--name-only", ref, "--", Dir)
	if err != nil {
		return nil, err
	}
	base := map[string]bool{}
	for _, f := range strings.Split(strings.TrimSpace(string(listed)), "\n") {
		if strings.HasSuffix(strings.ToLower(f), Suffix) {
			base[f] = true
		}
	}
	files, err := Files(root)
	if err != nil {
		return nil, err
	}
	var out []FileDiff
	for _, f := range files {
		cur, err := odcs.ParseFile(filepath.Join(root, filepath.FromSlash(f)))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if !base[f] {
			out = append(out, FileDiff{File: f, Status: "added"})
			continue
		}
		delete(base, f)
		raw, err := git(ctx, root, "show", ref+":./"+f)
		if err != nil {
			return nil, err
		}
		old, err := odcs.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%s at %s: %w", f, ref, err)
		}
		d := FileDiff{File: f, Status: "unchanged", Changes: odcs.Diff(old, cur)}
		if len(d.Changes) > 0 {
			d.Status = "changed"
		}
		out = append(out, d)
	}
	for f := range base {
		out = append(out, FileDiff{File: f, Status: "removed"})
	}
	sortDiffs(out)
	return out, nil
}

func sortDiffs(d []FileDiff) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j].File < d[j-1].File; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}
