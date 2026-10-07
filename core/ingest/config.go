// Package ingest loads ingestion definitions (ingest/*.yaml) and runs them:
// a KindSource module produces rows, the core loads them into the Bronze
// schema of the lake through a contract.Engine, and remembers incremental
// state in the Duct-owned table duct.duct_ingest_state. It has no cgo and
// does not import any concrete module.
package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Dir is the folder of a project that holds the ingestion definitions.
const Dir = "ingest"

// Modes of an ingestion.
const (
	// ModeFull replaces the table content on every run.
	ModeFull = "full"
	// ModeIncremental appends only rows whose watermark column is above the
	// highest value loaded by the last successful run.
	ModeIncremental = "incremental"
)

// BronzeSchema is the only schema ingestions may write to.
const BronzeSchema = "bronze"

// Config is one ingest/<name>.yaml file (schema: specs/ingest.schema.json).
type Config struct {
	Name      string         `yaml:"name"`
	Source    string         `yaml:"source"`
	Target    string         `yaml:"target"`
	Mode      string         `yaml:"mode"`
	Watermark string         `yaml:"watermark"`
	Options   map[string]any `yaml:"options"`

	// File is the path the definition was read from (not part of the YAML).
	File string `yaml:"-"`
}

var (
	nameRe  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// Table returns the target as schema and table (already validated).
func (c Config) Table() (schema, table string) {
	s, t, _ := strings.Cut(c.Target, ".")
	return s, t
}

// Validate checks the definition and fills in the target default.
func (c *Config) Validate() error {
	if !nameRe.MatchString(c.Name) {
		return fmt.Errorf("name %q must match %s", c.Name, nameRe)
	}
	if c.Source == "" {
		return errors.New("source is required (files, odbc or bento)")
	}
	if c.Target == "" {
		c.Target = BronzeSchema + "." + c.Name
	}
	schema, table, ok := strings.Cut(c.Target, ".")
	if !ok || schema != BronzeSchema || !nameRe.MatchString(table) {
		return fmt.Errorf("target %q must be %s.<table> (lowercase letters, digits, _)", c.Target, BronzeSchema)
	}
	switch c.Mode {
	case "":
		c.Mode = ModeFull
	case ModeFull, ModeIncremental:
	default:
		return fmt.Errorf("mode %q must be %q or %q", c.Mode, ModeFull, ModeIncremental)
	}
	if c.Mode == ModeIncremental && c.Watermark == "" {
		return errors.New("mode incremental requires watermark (column name)")
	}
	if c.Watermark != "" && !identRe.MatchString(c.Watermark) {
		return fmt.Errorf("watermark %q must be a plain column name", c.Watermark)
	}
	if c.Mode == ModeFull && c.Watermark != "" {
		return errors.New("watermark only makes sense with mode incremental")
	}
	return nil
}

// Parse decodes and validates one definition. Unknown keys are errors.
func Parse(data []byte) (Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return c, errors.New("empty file")
		}
		return c, err
	}
	return c, c.Validate()
}

// Load reads every ingest/*.yaml (and .yml) of the project root, sorted by
// name. A missing folder yields no configs. Names must be unique.
func Load(root string) ([]Config, error) {
	dir := filepath.Join(root, Dir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Config
	seen := map[string]string{}
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		if e.IsDir() || (ext != ".yaml" && ext != ".yml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path) //nolint:gosec // inside the project
		if err != nil {
			return nil, err
		}
		c, err := Parse(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(Dir, e.Name()), err)
		}
		c.File = path
		if prev, dup := seen[c.Name]; dup {
			return nil, fmt.Errorf("%s: name %q already defined in %s", e.Name(), c.Name, prev)
		}
		seen[c.Name] = e.Name()
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
