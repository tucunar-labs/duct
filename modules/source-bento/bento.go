// Package sourcebento embeds Bento (github.com/warpstreamlabs/bento, MIT) as a
// library to ingest REST (and any other Bento input) into Bronze. The user
// supplies a Bento input (plus optional processors); Duct replaces the output
// with a collector that turns the JSON messages into Arrow batches
// (contract.Source). Only Bento's `pure` and `io` component sets are linked.
package sourcebento

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/warpstreamlabs/bento/public/service"
	"gopkg.in/yaml.v3"

	// Keep Bento's footprint small: base components and std-lib IO only
	// (http_client, file, stdin, ...). Add sets here deliberately.
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindSource,
		Name: "bento",
		New:  func() contract.Module { return &Source{} },
	})
}

// batchRows is the number of rows per emitted Arrow batch.
const batchRows = 10000

// Source runs a Bento stream until its input ends.
type Source struct {
	input      string
	processors []string
}

var _ contract.Source = (*Source)(nil)

// Name implements contract.Module.
func (*Source) Name() string { return "bento" }

// Init reads options {input, processors}: input is a Bento input component
// (map, or a YAML string); processors is an optional list of Bento processors.
func (s *Source) Init(_ context.Context, cfg contract.Config) error {
	opts, _ := cfg["options"].(map[string]any)
	in, ok := opts["input"]
	if !ok {
		return errors.New("source-bento: options.input (a Bento input) is required")
	}
	var err error
	if s.input, err = toYAML(in); err != nil {
		return fmt.Errorf("source-bento: input: %w", err)
	}
	if ps, ok := opts["processors"]; ok {
		list, ok := ps.([]any)
		if !ok {
			return errors.New("source-bento: options.processors must be a list")
		}
		for i, p := range list {
			y, err := toYAML(p)
			if err != nil {
				return fmt.Errorf("source-bento: processors[%d]: %w", i, err)
			}
			s.processors = append(s.processors, y)
		}
	}
	// Fail early on malformed components instead of at the first Read.
	_, err = s.build(func(context.Context, service.MessageBatch) error { return nil })
	return err
}

func toYAML(v any) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	b, err := yaml.Marshal(v)
	return string(b), err
}

func (s *Source) build(consume service.MessageBatchHandlerFunc) (*service.Stream, error) {
	b := service.NewStreamBuilder()
	if err := b.SetLoggerYAML("level: WARN"); err != nil {
		return nil, err
	}
	if err := b.AddInputYAML(s.input); err != nil {
		return nil, fmt.Errorf("source-bento: input: %w", err)
	}
	for _, p := range s.processors {
		if err := b.AddProcessorYAML(p); err != nil {
			return nil, fmt.Errorf("source-bento: processor: %w", err)
		}
	}
	if err := b.AddBatchConsumerFunc(consume); err != nil {
		return nil, err
	}
	return b.Build()
}

// Read runs the stream to completion, collecting every message, then emits
// the rows as Arrow batches with one schema inferred over all of them
// (columns in first-seen order; int64, float64, bool or string; nested values
// and mixed types become JSON text). Bento is not started again after Read.
func (s *Source) Read(ctx context.Context, emit func(contract.Batch) error) error {
	var rows []map[string]any
	var order []string
	seen := map[string]bool{}
	add := func(m map[string]any) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic within one row
		for _, k := range keys {
			if !seen[k] {
				seen[k] = true
				order = append(order, k)
			}
		}
		rows = append(rows, m)
	}
	stream, err := s.build(func(_ context.Context, batch service.MessageBatch) error {
		for _, msg := range batch {
			v, err := msg.AsStructured()
			if err != nil {
				return fmt.Errorf("message is not JSON: %w", err)
			}
			switch x := v.(type) {
			case map[string]any:
				add(x)
			case []any: // a REST array response: one row per element
				for _, e := range x {
					if m, ok := e.(map[string]any); ok {
						add(m)
					} else {
						add(map[string]any{"value": e})
					}
				}
			default:
				add(map[string]any{"value": x})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := stream.Run(ctx); err != nil {
		return fmt.Errorf("source-bento: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	return emitRows(rows, order, emit)
}

// Close implements contract.Module.
func (*Source) Close() error { return nil }

type colKind int

const (
	kNone colKind = iota
	kInt
	kFloat
	kBool
	kString
)

func kindOf(v any) colKind {
	switch x := v.(type) {
	case nil:
		return kNone
	case bool:
		return kBool
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return kInt
		}
		return kFloat
	case int, int32, int64, uint, uint32, uint64:
		return kInt
	case float32, float64:
		if f, ok := x.(float64); ok && f == float64(int64(f)) && f >= -1<<53 && f <= 1<<53 {
			return kInt
		}
		return kFloat
	}
	return kString
}

func merge(a, b colKind) colKind {
	switch {
	case a == kNone:
		return b
	case b == kNone || a == b:
		return a
	case (a == kInt && b == kFloat) || (a == kFloat && b == kInt):
		return kFloat
	}
	return kString
}

func emitRows(rows []map[string]any, order []string, emit func(contract.Batch) error) error {
	kinds := make([]colKind, len(order))
	for i, name := range order {
		for _, r := range rows {
			kinds[i] = merge(kinds[i], kindOf(r[name]))
		}
		if kinds[i] == kNone {
			kinds[i] = kString // all-null column
		}
	}
	fields := make([]arrow.Field, len(order))
	for i, name := range order {
		fields[i] = arrow.Field{Name: name, Type: map[colKind]arrow.DataType{
			kInt: arrow.PrimitiveTypes.Int64, kFloat: arrow.PrimitiveTypes.Float64,
			kBool: arrow.FixedWidthTypes.Boolean, kString: arrow.BinaryTypes.String,
		}[kinds[i]], Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	for start := 0; start < len(rows); start += batchRows {
		end := min(start+batchRows, len(rows))
		bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for _, r := range rows[start:end] {
			for i, name := range order {
				if err := appendValue(bld.Field(i), kinds[i], r[name]); err != nil {
					bld.Release()
					return fmt.Errorf("column %s: %w", name, err)
				}
			}
		}
		rec := bld.NewRecordBatch()
		bld.Release()
		err := emit(rec)
		rec.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

func appendValue(b array.Builder, k colKind, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch k {
	case kInt:
		n, err := toInt(v)
		if err != nil {
			return err
		}
		b.(*array.Int64Builder).Append(n)
	case kFloat:
		f, err := toFloat(v)
		if err != nil {
			return err
		}
		b.(*array.Float64Builder).Append(f)
	case kBool:
		b.(*array.BooleanBuilder).Append(v.(bool))
	default:
		if s, ok := v.(string); ok {
			b.(*array.StringBuilder).Append(s)
			return nil
		}
		j, err := json.Marshal(v)
		if err != nil {
			return err
		}
		b.(*array.StringBuilder).Append(string(j))
	}
	return nil
}

func toInt(v any) (int64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Int64()
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil //nolint:gosec // JSON ints
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil //nolint:gosec // JSON ints
	case float32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	}
	return 0, fmt.Errorf("not an integer: %v", v)
}

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Float64()
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	}
	n, err := toInt(v)
	return float64(n), err
}
