package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Metadata columns added to every landed row. Payload fields with these names
// (and any starting with "_kafka_") are dropped.
const (
	ColPartition = "_kafka_partition"
	ColOffset    = "_kafka_offset"
	ColTimestamp = "_kafka_ts"
	ColKey       = "_kafka_key"
	ColIngested  = "_ingested_at" // now() of the landing transaction
)

// Reject is a message that is not a JSON object.
type Reject struct {
	Rec Record
	Err string
}

// Decoded is a micro-batch ready to land.
type Decoded struct {
	// Rec holds the valid messages (nil if there are none): one column per
	// JSON field (first-appearance order) then the metadata columns. The
	// caller releases it.
	Rec arrow.RecordBatch
	// JSONCols are the columns holding nested objects/arrays as JSON text;
	// the landing SQL casts them to JSON.
	JSONCols map[string]bool
	Rejects  []Reject
}

type kind int

const (
	kNull kind = iota
	kBool
	kInt
	kFloat
	kTime
	kString
	kJSON
)

func merge(a, b kind) kind {
	switch {
	case a == b, b == kNull:
		return a
	case a == kNull:
		return b
	case (a == kInt && b == kFloat) || (a == kFloat && b == kInt):
		return kFloat
	}
	return kString // anything else is kept as its JSON/text form
}

func kindOf(v any) kind {
	switch x := v.(type) {
	case nil:
		return kNull
	case bool:
		return kBool
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return kInt
		}
		return kFloat
	case string:
		if isTimestamp(x) {
			return kTime
		}
		return kString
	}
	return kJSON
}

func isTimestamp(s string) bool {
	if len(s) < 20 || s[4] != '-' || s[10] != 'T' {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, s)
	return err == nil
}

// text is the string form of v for columns whose kind ended up as kString.
func text(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case json.Number:
		return x.String()
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func reserved(name string) bool {
	return name == ColIngested || strings.HasPrefix(name, "_kafka_")
}

// decodeObject parses one JSON object keeping the key order of the message.
func decodeObject(b []byte) ([]string, map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	t, err := dec.Token()
	if err != nil {
		return nil, nil, err
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, nil, errors.New("message is not a JSON object")
	}
	var keys []string
	vals := map[string]any{}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, nil, err
		}
		k, _ := kt.(string)
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, nil, err
		}
		if _, dup := vals[k]; !dup {
			keys = append(keys, k)
		}
		vals[k] = v
	}
	if _, err := dec.Token(); err != nil { // closing brace
		return nil, nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("trailing data after the JSON object")
	}
	return keys, vals, nil
}

type row struct {
	rec  Record
	keys []string
	vals map[string]any
}

// Decode turns messages into one Arrow batch. Column types are inferred over
// the whole batch: bool, int64, float64 (ints and floats mix into float64),
// timestamp (every string is RFC 3339), string; nested values are JSON text;
// any other mix falls back to text. A field that is null in every message
// of the batch gets no column. Invalid messages become Rejects.
func Decode(recs []Record, mem memory.Allocator) (*Decoded, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	out := &Decoded{JSONCols: map[string]bool{}}
	var rows []row
	var order []string
	kinds := map[string]kind{}
	for _, r := range recs {
		if len(bytes.TrimSpace(r.Value)) == 0 {
			out.Rejects = append(out.Rejects, Reject{r, "empty message"})
			continue
		}
		keys, vals, err := decodeObject(r.Value)
		if err != nil {
			out.Rejects = append(out.Rejects, Reject{r, err.Error()})
			continue
		}
		rows = append(rows, row{r, keys, vals})
		for _, k := range keys {
			if reserved(k) {
				continue
			}
			if _, seen := kinds[k]; !seen {
				order = append(order, k)
			}
			kinds[k] = merge(kinds[k], kindOf(vals[k]))
		}
	}
	if len(rows) == 0 {
		return out, nil
	}

	var cols []string
	fields := make([]arrow.Field, 0, len(order)+4)
	for _, k := range order {
		var dt arrow.DataType
		switch kinds[k] {
		case kNull:
			continue
		case kBool:
			dt = arrow.FixedWidthTypes.Boolean
		case kInt:
			dt = arrow.PrimitiveTypes.Int64
		case kFloat:
			dt = arrow.PrimitiveTypes.Float64
		case kTime:
			dt = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		case kString:
			dt = arrow.BinaryTypes.String
		case kJSON:
			dt = arrow.BinaryTypes.String
			out.JSONCols[k] = true
		}
		cols = append(cols, k)
		fields = append(fields, arrow.Field{Name: k, Type: dt, Nullable: true})
	}
	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	fields = append(fields,
		arrow.Field{Name: ColPartition, Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: ColOffset, Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: ColTimestamp, Type: ts},
		arrow.Field{Name: ColKey, Type: arrow.BinaryTypes.String, Nullable: true})

	rb := array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil))
	defer rb.Release()
	for _, r := range rows {
		for i, k := range cols {
			if err := appendValue(rb.Field(i), kinds[k], r.vals[k]); err != nil {
				return nil, fmt.Errorf("offset %d field %q: %w", r.rec.Offset, k, err)
			}
		}
		n := len(cols)
		rb.Field(n).(*array.Int32Builder).Append(r.rec.Partition)
		rb.Field(n + 1).(*array.Int64Builder).Append(r.rec.Offset)
		rb.Field(n + 2).(*array.TimestampBuilder).Append(arrow.Timestamp(r.rec.Timestamp.UnixMicro()))
		kb := rb.Field(n + 3).(*array.StringBuilder)
		if r.rec.Key == nil {
			kb.AppendNull()
		} else {
			kb.Append(string(r.rec.Key))
		}
	}
	out.Rec = rb.NewRecordBatch()
	return out, nil
}

func appendValue(b array.Builder, k kind, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch k {
	case kBool:
		b.(*array.BooleanBuilder).Append(v.(bool))
	case kInt:
		n, err := v.(json.Number).Int64()
		if err != nil {
			return err
		}
		b.(*array.Int64Builder).Append(n)
	case kFloat:
		f, err := v.(json.Number).Float64()
		if err != nil {
			return err
		}
		b.(*array.Float64Builder).Append(f)
	case kTime:
		t, err := time.Parse(time.RFC3339Nano, v.(string))
		if err != nil {
			return err
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMicro()))
	default: // kString, kJSON
		b.(*array.StringBuilder).Append(text(v))
	}
	return nil
}
