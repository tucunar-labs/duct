// Package contracts implements Duct data contracts (ADR 0004): ODCS v3.2
// files in contracts/<table>.odcs.yaml, parsed with odcs-go, compiled to
// schema checks, quality checks, grants and freshness rules, and enforced by
// `duct run` (see Enforcer). It has no cgo.
//
// The lake is referenced as a server of type "custom" whose customProperty
// "ducklake_table" names the table (schema.name). Duct extensions live in
// customProperties with names prefixed "duct." (see knownExtension).
package contracts

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	odcs "github.com/alonsoburon/odcs-go"

	"github.com/alonsoburon/duct/core/governance"
)

// Dir is the folder of contract files inside a project.
const Dir = "contracts"

// Suffix is the file name suffix of a contract.
const Suffix = ".odcs.yaml"

// PropTable is the customProperty of a custom server that names the DuckLake table.
const PropTable = "ducklake_table"

// Contract is one parsed contract bound to one lake table.
type Contract struct {
	File   string // path relative to the project root, slash separated
	DC     *odcs.DataContract
	Table  string // normalised schema.name
	Object *odcs.SchemaObject
}

// Files lists the contract files under root, sorted. A project without a
// contracts folder has none (not an error: contracts are optional).
func Files(root string) ([]string, error) {
	base := filepath.Join(root, Dir)
	if st, err := os.Stat(base); err != nil || !st.IsDir() {
		return nil, nil
	}
	var out []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(strings.ToLower(p), Suffix) {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// Parse validates data against the ODCS schema, decodes it and resolves the
// table it governs. file is only used in errors.
func Parse(file string, data []byte) (*Contract, error) {
	if err := odcs.Validate(data); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	dc, err := odcs.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return bind(file, dc)
}

// bind resolves the table of an already decoded contract.
func bind(file string, dc *odcs.DataContract) (*Contract, error) {
	c := &Contract{File: file, DC: dc}
	if len(dc.Schema) != 1 {
		return c, fmt.Errorf("%s: a Duct contract describes exactly one table (schema has %d objects)", file, len(dc.Schema))
	}
	c.Object = &dc.Schema[0]
	rel := ""
	for _, s := range dc.Servers {
		if s.Type != "custom" {
			continue
		}
		if v := customString(s.CustomProperties, PropTable); v != "" {
			rel = v
			break
		}
	}
	if rel == "" {
		rel = c.Object.PhysicalName
	}
	if rel == "" {
		rel = c.Object.Name
	}
	s, t, err := governance.NormalizeRelation(rel)
	if err != nil {
		return c, fmt.Errorf("%s: table: %w", file, err)
	}
	c.Table = s + "." + t
	return c, nil
}

// Load parses every contract of the project and rejects two contracts for
// the same table.
func Load(root string) ([]*Contract, error) {
	files, err := Files(root)
	if err != nil {
		return nil, err
	}
	var out []*Contract
	seen := map[string]string{}
	var errs []error
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f))) //nolint:gosec // walked under the project root
		if err != nil {
			return nil, err
		}
		c, err := Parse(f, data)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if prev, dup := seen[c.Table]; dup {
			errs = append(errs, fmt.Errorf("%s: %s is already governed by %s", f, c.Table, prev))
			continue
		}
		seen[c.Table] = f
		out = append(out, c)
	}
	return out, errors.Join(errs...)
}

// ByTable indexes contracts by table id.
func ByTable(cs []*Contract) map[string]*Contract {
	m := make(map[string]*Contract, len(cs))
	for _, c := range cs {
		m[c.Table] = c
	}
	return m
}

func customString(props []odcs.CustomProperty, name string) string {
	for _, p := range props {
		if p.Property == name {
			if s, ok := p.Value.(string); ok {
				return strings.TrimSpace(s)
			}
			return strings.TrimSpace(fmt.Sprint(p.Value))
		}
	}
	return ""
}

// Duct extension names (customProperties).
const (
	extPrefix     = "duct."
	extRowFilter  = "duct.row_filter"   // on a role: SQL predicate
	extMaskPrefix = "duct.mask."        // on a role: duct.mask.<column> = SQL expression
	extOnViolate  = "duct.on_violation" // on a quality rule: drop_row
)

// knownExtension reports whether a "duct."-prefixed property name is understood
// in the given place ("role" or "quality").
func knownExtension(place, name string) bool {
	switch place {
	case "role":
		return name == extRowFilter || strings.HasPrefix(name, extMaskPrefix) && len(name) > len(extMaskPrefix)
	case "quality":
		return name == extOnViolate
	}
	return false
}
