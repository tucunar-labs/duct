package udf

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// execSpec is functions/<name>.toml:
//
//	name    = "slug"                        # default: the file name
//	command = ["tr", "A-Z", "a-z"]          # argv, no shell; run from the project root
//	args    = ["VARCHAR"]                   # SQL types of the arguments
//	returns = "VARCHAR"
//	format  = "tsv"                         # tsv (default) or json
//	timeout = "10s"                         # per batch
//
// The command is started once per batch (like a ClickHouse executable UDF
// without pool), receives one row per line on stdin and must write exactly
// one result per line on stdout, in order. tsv: values separated by a tab
// (\t, \n, \r and \ escaped); json: one JSON array per row in, one JSON value
// per line out. Besides the scalars, DATE ("2026-01-31"), TIMESTAMP
// ("2026-01-31 12:30:00.123456"), DECIMAL(p,s) (exact text in tsv, a JSON number in
// json) work in both formats; lists (JSON arrays) and STRUCT (JSON objects) only
// with format = "json".
type execSpec struct {
	Name    string   `toml:"name"`
	Command []string `toml:"command"`
	Args    []string `toml:"args"`
	Returns string   `toml:"returns"`
	Format  string   `toml:"format"`
	Timeout string   `toml:"timeout"`

	argT []*contract.SQLType
	retT *contract.SQLType
}

func loadExecSpec(path string) (Spec, error) {
	var e execSpec
	md, err := toml.DecodeFile(path, &e)
	if err != nil {
		return Spec{}, fmt.Errorf("udf: %s: %w", filepath.Base(path), err)
	}
	if u := md.Undecoded(); len(u) > 0 {
		return Spec{}, fmt.Errorf("udf: %s: unknown keys %v", filepath.Base(path), u)
	}
	if e.Name == "" {
		e.Name = strings.TrimSuffix(filepath.Base(path), ".toml")
	}
	if len(e.Command) == 0 || e.Command[0] == "" {
		return Spec{}, fmt.Errorf("udf: %s: command (a list: [\"cmd\", \"arg\"...]) is required", filepath.Base(path))
	}
	if e.Returns == "" {
		return Spec{}, fmt.Errorf("udf: %s: returns is required", filepath.Base(path))
	}
	switch e.Format {
	case "":
		e.Format = "tsv"
	case "tsv", "json":
	default:
		return Spec{}, fmt.Errorf("udf: %s: format must be tsv or json", filepath.Base(path))
	}
	if e.Timeout != "" {
		if _, err := time.ParseDuration(e.Timeout); err != nil {
			return Spec{}, fmt.Errorf("udf: %s: timeout: %w", filepath.Base(path), err)
		}
	}
	spec := Spec{Name: e.Name, Kind: "exec", exec: &e}
	for i, a := range e.Args {
		t, err := contract.ParseSQLType(a)
		if err != nil {
			return Spec{}, fmt.Errorf("udf: %s: args[%d]: %w", filepath.Base(path), i, err)
		}
		if e.Format == "tsv" && !t.IsScalar() {
			return Spec{}, fmt.Errorf("udf: %s: %s needs format = \"json\" (tsv only carries scalars)", filepath.Base(path), t)
		}
		e.argT = append(e.argT, t)
		spec.ArgTypes = append(spec.ArgTypes, t.String())
	}
	t, err := contract.ParseSQLType(e.Returns)
	if err != nil {
		return Spec{}, fmt.Errorf("udf: %s: returns: %w", filepath.Base(path), err)
	}
	if e.Format == "tsv" && !t.IsScalar() {
		return Spec{}, fmt.Errorf("udf: %s: returns %s needs format = \"json\" (tsv only carries scalars)", filepath.Base(path), t)
	}
	e.retT, spec.ReturnType = t, t.String()
	return spec, nil
}

func (e *execSpec) caller(opt Options, s Spec) func(context.Context, []arrow.Array) (arrow.Array, error) {
	timeout := opt.Timeout
	if e.Timeout != "" {
		timeout, _ = time.ParseDuration(e.Timeout)
	}
	return func(ctx context.Context, args []arrow.Array) (arrow.Array, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		n := args[0].Len()
		in := e.encode(args, n)
		cmd := exec.CommandContext(ctx, e.Command[0], e.Command[1:]...) //nolint:gosec // argv from the project's own functions/*.toml
		cmd.Dir = opt.Root
		cmd.Env = SafeEnv(nil)
		cmd.Stdin = bytes.NewReader(in)
		var out, errb bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &errb
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("%s: %w: %s", e.Command[0], err, tail(errb.String()))
		}
		return e.decode(&out, n)
	}
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 500 {
		s = "..." + s[len(s)-500:]
	}
	return s
}

func (e *execSpec) encode(args []arrow.Array, n int) []byte {
	var b bytes.Buffer
	for i := range n {
		if e.Format == "json" {
			row := make([]any, len(args))
			for c, a := range args {
				row[c] = jsonValue(a, i)
			}
			enc, _ := json.Marshal(row)
			b.Write(enc)
		} else {
			for c, a := range args {
				if c > 0 {
					b.WriteByte('\t')
				}
				b.WriteString(escapeTSV(fmt.Sprint(jsonValue(a, i))))
			}
		}
		b.WriteByte('\n')
	}
	return b.Bytes()
}

var tsvEscaper = strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`)
var tsvUnescaper = strings.NewReplacer(`\\`, `\`, `\t`, "\t", `\n`, "\n", `\r`, "\r")

func escapeTSV(s string) string   { return tsvEscaper.Replace(s) }
func unescapeTSV(s string) string { return tsvUnescaper.Replace(s) }

// jsonValue is element i of a as a value that marshals to the exec wire form (and
// prints as the tsv text): DATE and TIMESTAMP as strings, DECIMAL as an exact number.
func jsonValue(a arrow.Array, i int) any {
	if a.IsNull(i) {
		return nil
	}
	switch x := a.(type) {
	case *array.Boolean:
		return x.Value(i)
	case *array.Int8:
		return x.Value(i)
	case *array.Int16:
		return x.Value(i)
	case *array.Int32:
		return x.Value(i)
	case *array.Int64:
		return x.Value(i)
	case *array.Float32:
		return x.Value(i)
	case *array.Float64:
		return x.Value(i)
	case *array.String:
		return x.Value(i)
	case *array.Date32:
		return x.Value(i).ToTime().Format("2006-01-02")
	case *array.Timestamp:
		return x.Value(i).ToTime(arrow.Microsecond).Format("2006-01-02 15:04:05.999999")
	case *array.Decimal128:
		return json.Number(x.Value(i).ToString(x.DataType().(*arrow.Decimal128Type).Scale))
	case *array.List:
		start, end := x.ValueOffsets(i)
		out := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, jsonValue(x.ListValues(), int(j)))
		}
		return out
	case *array.Struct:
		st := x.DataType().(*arrow.StructType)
		out := make(map[string]any, len(st.Fields()))
		for k, f := range st.Fields() {
			out[f.Name] = jsonValue(x.Field(k), i)
		}
		return out
	}
	return nil
}

func (e *execSpec) decode(out *bytes.Buffer, n int) (arrow.Array, error) {
	ret := e.retT
	viaJSON := arrowJSON(ret)
	var bld array.Builder
	var texts []string
	if !viaJSON {
		bld = array.NewBuilder(memory.DefaultAllocator, ret.Arrow())
		defer bld.Release()
	}
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 64*1024), 16<<20)
	got := 0
	for sc.Scan() {
		got++
		if got > n {
			return nil, fmt.Errorf("%s wrote more than %d lines", e.Command[0], n)
		}
		line := sc.Text()
		if viaJSON {
			text := line
			if e.Format == "tsv" { // a scalar: quote it so Arrow's JSON reader sees a string
				text = strconv.Quote(unescapeTSV(line))
			}
			if !json.Valid([]byte(text)) {
				return nil, fmt.Errorf("%s line %d: not a valid %s: %q", e.Command[0], got, ret, line)
			}
			texts = append(texts, text)
			continue
		}
		if err := e.appendLine(bld, ret, line); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", e.Command[0], got, err)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if got != n {
		return nil, fmt.Errorf("%s wrote %d lines for %d rows (it must answer one line per row)", e.Command[0], got, n)
	}
	if viaJSON {
		arr, _, err := array.FromJSON(memory.DefaultAllocator, ret.Arrow(), strings.NewReader("["+strings.Join(texts, ",")+"]"))
		if err != nil {
			return nil, fmt.Errorf("%s: not a valid %s: %w", e.Command[0], ret, err)
		}
		return arr, nil
	}
	return bld.NewArray(), nil
}

// arrowJSON reports whether results of type t are parsed by Arrow's JSON reader
// (the plain scalars keep the lenient parsing of appendParsed).
func arrowJSON(t *contract.SQLType) bool {
	switch t.Kind {
	case "DATE", "TIMESTAMP", "DECIMAL", "LIST", "STRUCT":
		return true
	}
	return false
}

// appendLine parses one output line of a plain scalar type (a tsv "42" or a JSON
// 42 or "42" are all fine).
func (e *execSpec) appendLine(b array.Builder, t *contract.SQLType, line string) error {
	var v any = line
	if e.Format == "json" {
		dec := json.NewDecoder(strings.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf("not JSON: %w", err)
		}
	} else {
		v = unescapeTSV(line)
	}
	return appendParsed(b, t.Kind, v)
}

// appendParsed appends v (a string in tsv, any JSON value in json) as type t.
func appendParsed(b array.Builder, t string, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	str := func() string {
		if s, ok := v.(string); ok {
			return s
		}
		enc, _ := json.Marshal(v)
		return string(enc)
	}
	switch bb := b.(type) {
	case *array.StringBuilder:
		bb.Append(str())
	case *array.BooleanBuilder:
		switch x := v.(type) {
		case bool:
			bb.Append(x)
		default:
			p, err := strconv.ParseBool(str())
			if err != nil {
				return fmt.Errorf("not a boolean: %q", str())
			}
			bb.Append(p)
		}
	case *array.Float32Builder:
		f, err := strconv.ParseFloat(str(), 32)
		if err != nil {
			return fmt.Errorf("not a number: %q", str())
		}
		bb.Append(float32(f))
	case *array.Float64Builder:
		f, err := strconv.ParseFloat(str(), 64)
		if err != nil {
			return fmt.Errorf("not a number: %q", str())
		}
		bb.Append(f)
	default:
		bits := map[string]int{"TINYINT": 8, "SMALLINT": 16, "INTEGER": 32, "BIGINT": 64}[t]
		i, err := strconv.ParseInt(strings.TrimSpace(str()), 10, bits)
		if err != nil {
			return fmt.Errorf("not a %s: %q", strings.ToLower(t), str())
		}
		switch ib := b.(type) {
		case *array.Int8Builder:
			ib.Append(int8(i))
		case *array.Int16Builder:
			ib.Append(int16(i))
		case *array.Int32Builder:
			ib.Append(int32(i))
		case *array.Int64Builder:
			ib.Append(i)
		default:
			return errors.New("unsupported result type " + t)
		}
	}
	return nil
}
