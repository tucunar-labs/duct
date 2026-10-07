// Package stream lands Kafka topics in Bronze (ADR 0003): a consumer group
// reads the topics declared in streams/*.yaml, turns JSON messages into Arrow
// micro-batches, writes each batch to DuckLake in one transaction and only
// then commits the Kafka offsets. It has no cgo and does not import any
// concrete module; the Kafka client is behind the Consumer interface.
package stream

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
	"time"

	"gopkg.in/yaml.v3"
)

// Dir is the folder of a project that holds the stream definitions.
const Dir = "streams"

// BronzeSchema is the only schema streams may write to.
const BronzeSchema = "bronze"

// Defaults of a stream definition.
const (
	DefaultMaxRows     = 5000
	DefaultMaxInterval = time.Second
)

// Batch bounds one micro-batch: it is flushed when MaxRows messages are
// buffered or MaxInterval has passed since the first one, whichever is first.
type Batch struct {
	MaxRows     int           `yaml:"max_rows"`
	MaxInterval time.Duration `yaml:"max_interval"`
}

// Config is one streams/<name>.yaml file (schema: specs/stream.schema.json).
type Config struct {
	Name   string `yaml:"name"`
	Topic  string `yaml:"topic"`
	Target string `yaml:"target"` // bronze.<table>; default bronze.<name>
	Format string `yaml:"format"` // json (only)
	Batch  Batch  `yaml:"batch"`
	// Group is the Kafka consumer group of the writer (default duct-stream-<name>).
	Group string `yaml:"group"`
	// Start is where a group with no committed offset begins: earliest
	// (default) or latest.
	Start string `yaml:"start"`
	// InlineRows, if > 0, sets DuckLake's data_inlining_row_limit for the
	// bronze schema (a lake-wide set_option crashes DuckDB 1.5.6 once tables
	// exist) so micro-batches up to that size go to the catalog instead of
	// producing one tiny Parquet file each (`duct gc` flushes them).
	InlineRows int `yaml:"inline_rows"`
	// Window is how much history `duct alerts` keeps in memory for this
	// stream (a table named after the stream). Zero: alerts ignore the stream.
	Window time.Duration `yaml:"window"`

	// File is the path the definition was read from (not part of the YAML).
	File string `yaml:"-"`
}

var nameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Table returns the target table name (already validated).
func (c Config) Table() string {
	_, t, _ := strings.Cut(c.Target, ".")
	return t
}

// Validate checks the definition and fills in the defaults.
func (c *Config) Validate() error {
	if !nameRe.MatchString(c.Name) {
		return fmt.Errorf("name %q must match %s", c.Name, nameRe)
	}
	if strings.TrimSpace(c.Topic) == "" {
		return errors.New("topic is required")
	}
	if c.Target == "" {
		c.Target = BronzeSchema + "." + c.Name
	}
	schema, table, ok := strings.Cut(c.Target, ".")
	if !ok || schema != BronzeSchema || !nameRe.MatchString(table) {
		return fmt.Errorf("target %q must be %s.<table> (lowercase letters, digits, _)", c.Target, BronzeSchema)
	}
	switch c.Format {
	case "":
		c.Format = "json"
	case "json":
	default:
		return fmt.Errorf("format %q is not supported (only json)", c.Format)
	}
	if c.Batch.MaxRows < 0 || c.Batch.MaxInterval < 0 {
		return errors.New("batch limits cannot be negative")
	}
	if c.Batch.MaxRows == 0 {
		c.Batch.MaxRows = DefaultMaxRows
	}
	if c.Batch.MaxInterval == 0 {
		c.Batch.MaxInterval = DefaultMaxInterval
	}
	if c.Group == "" {
		c.Group = "duct-stream-" + c.Name
	}
	switch c.Start {
	case "":
		c.Start = "earliest"
	case "earliest", "latest":
	default:
		return fmt.Errorf("start %q must be earliest or latest", c.Start)
	}
	if c.InlineRows < 0 || c.Window < 0 {
		return errors.New("inline_rows and window cannot be negative")
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

// Load reads every streams/*.yaml (and .yml) of the project root, sorted by
// name. A missing folder yields no configs. Names, topics and targets must be
// unique.
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
	names, topics, targets := map[string]string{}, map[string]string{}, map[string]string{}
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
		for _, u := range []struct {
			what, val string
			seen      map[string]string
		}{{"name", c.Name, names}, {"topic", c.Topic, topics}, {"target", c.Target, targets}} {
			if prev, dup := u.seen[u.val]; dup {
				return nil, fmt.Errorf("%s: %s %q already used in %s", e.Name(), u.what, u.val, prev)
			}
			u.seen[u.val] = e.Name()
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
