package pipeline

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Dir is the folder of pipeline files inside a project.
const Dir = "pipelines"

// Load parses every pipelines/**/*.sql under root, in path order. Defining
// the same table twice is an error. A project without a pipelines folder is
// an error too: there is nothing to run.
func Load(root string) ([]Table, error) {
	base := filepath.Join(root, Dir)
	if st, err := os.Stat(base); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("no %s/ folder in the project: put your .sql pipeline files there", Dir)
	}
	var files []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.EqualFold(filepath.Ext(p), ".sql") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var out []Table
	first := map[string]string{}
	for _, f := range files {
		src, err := os.ReadFile(f) //nolint:gosec // walked under the project root
		if err != nil {
			return nil, err
		}
		rel, _ := filepath.Rel(root, f)
		ts, err := Parse(filepath.ToSlash(rel), string(src))
		if err != nil {
			return nil, err
		}
		for _, t := range ts {
			if prev, dup := first[t.ID()]; dup {
				return nil, fmt.Errorf("%s:%d: %s is already defined in %s", t.File, t.Line, t.ID(), prev)
			}
			first[t.ID()] = fmt.Sprintf("%s:%d", t.File, t.Line)
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s/ has no CREATE MATERIALIZED VIEW or CREATE STREAMING TABLE statements", Dir)
	}
	return out, nil
}
