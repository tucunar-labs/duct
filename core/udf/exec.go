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
// per line out.
type execSpec struct {
	Name    string   `toml:"name"`
	Command []string `toml:"command"`
	Args    []string `toml:"args"`
	Returns string   `toml:"returns"`
	Format  string   `toml:"format"`
	Timeout string   `toml:"timeout"`
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
	return Spec{Name: e.Name, ArgTypes: upper(e.Args), ReturnType: strings.ToUpper(e.Returns), Kind: "exec", exec: &e}, nil
}

func upper(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = strings.ToUpper(v)
	}
	return out
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
		return e.decode(&out, n, s.ReturnType)
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
				row[c] = value(a, i)
			}
			enc, _ := json.Marshal(row)
			b.Write(enc)
		} else {
			for c, a := range args {
				if c > 0 {
					b.WriteByte('\t')
				}
				b.WriteString(escapeTSV(fmt.Sprint(value(a, i))))
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

func value(a arrow.Array, i int) any {
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
	}
	return nil
}

func (e *execSpec) decode(out *bytes.Buffer, n int, ret string) (arrow.Array, error) {
	bld := array.NewBuilder(memory.DefaultAllocator, arrowOf(ret))
	defer bld.Release()
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 64*1024), 16<<20)
	got := 0
	for sc.Scan() {
		got++
		if got > n {
			return nil, fmt.Errorf("%s wrote more than %d lines", e.Command[0], n)
		}
		line := sc.Text()
		var v any = line
		if e.Format == "json" {
			dec := json.NewDecoder(strings.NewReader(line))
			dec.UseNumber()
			if err := dec.Decode(&v); err != nil {
				return nil, fmt.Errorf("%s line %d: not JSON: %w", e.Command[0], got, err)
			}
		} else {
			v = unescapeTSV(line)
		}
		if err := appendParsed(bld, ret, v); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", e.Command[0], got, err)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if got != n {
		return nil, fmt.Errorf("%s wrote %d lines for %d rows (it must answer one line per row)", e.Command[0], got, n)
	}
	return bld.NewArray(), nil
}

func arrowOf(t string) arrow.DataType {
	switch t {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	}
	return arrow.BinaryTypes.String
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
