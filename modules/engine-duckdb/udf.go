package engineduckdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

var _ contract.ScalarRegistrar = (*Engine)(nil)

// udfTypes are the SQL types a ScalarFunction may use (ADR 0005).
var udfTypes = map[string]struct {
	duck  duckdb.Type
	arrow arrow.DataType
}{
	"BOOLEAN":  {duckdb.TYPE_BOOLEAN, arrow.FixedWidthTypes.Boolean},
	"TINYINT":  {duckdb.TYPE_TINYINT, arrow.PrimitiveTypes.Int8},
	"SMALLINT": {duckdb.TYPE_SMALLINT, arrow.PrimitiveTypes.Int16},
	"INTEGER":  {duckdb.TYPE_INTEGER, arrow.PrimitiveTypes.Int32},
	"BIGINT":   {duckdb.TYPE_BIGINT, arrow.PrimitiveTypes.Int64},
	"FLOAT":    {duckdb.TYPE_FLOAT, arrow.PrimitiveTypes.Float32},
	"DOUBLE":   {duckdb.TYPE_DOUBLE, arrow.PrimitiveTypes.Float64},
	"VARCHAR":  {duckdb.TYPE_VARCHAR, arrow.BinaryTypes.String},
}

// RegisterScalar implements contract.ScalarRegistrar. DuckDB registers the
// function in the database catalog, so every pooled connection sees it.
func (e *Engine) RegisterScalar(ctx context.Context, f contract.ScalarFunction) error {
	db, err := e.handle()
	if err != nil {
		return err
	}
	u, err := newScalarUDF(f)
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := duckdb.RegisterScalarUDF(conn, f.Name, u); err != nil {
		return fmt.Errorf("engine-duckdb: register %s: %w", f.Name, err)
	}
	if f.Close != nil {
		e.mu.Lock()
		e.udfs = append(e.udfs, f.Close)
		e.mu.Unlock()
	}
	return nil
}

type scalarUDF struct {
	f    contract.ScalarFunction
	args []arrow.DataType
	ret  arrow.DataType
	cfg  duckdb.ScalarFuncConfig
}

func newScalarUDF(f contract.ScalarFunction) (*scalarUDF, error) {
	if f.Name == "" || f.Call == nil {
		return nil, fmt.Errorf("engine-duckdb: scalar function needs a name and Call")
	}
	u := &scalarUDF{f: f}
	for i, t := range f.ArgTypes {
		m, ok := udfTypes[strings.ToUpper(t)]
		if !ok {
			return nil, fmt.Errorf("engine-duckdb: function %s: argument %d has unsupported type %q", f.Name, i+1, t)
		}
		ti, err := duckdb.NewTypeInfo(m.duck)
		if err != nil {
			return nil, err
		}
		u.cfg.InputTypeInfos = append(u.cfg.InputTypeInfos, ti)
		u.args = append(u.args, m.arrow)
	}
	m, ok := udfTypes[strings.ToUpper(f.ReturnType)]
	if !ok {
		return nil, fmt.Errorf("engine-duckdb: function %s: unsupported return type %q", f.Name, f.ReturnType)
	}
	ti, err := duckdb.NewTypeInfo(m.duck)
	if err != nil {
		return nil, err
	}
	u.cfg.ResultTypeInfo = ti
	u.ret = m.arrow
	return u, nil
}

func (u *scalarUDF) Config() duckdb.ScalarFuncConfig { return u.cfg }

func (u *scalarUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{ChunkContextExecutor: u.execute}
}

// execute turns one input chunk into Arrow arrays, calls the function once
// and writes the result back. Rows with a NULL argument are skipped by Rows()
// (and set to NULL by DuckDB afterwards), so Call only sees complete rows.
func (u *scalarUDF) execute(ctx context.Context, st *duckdb.ChunkIteratorState) error {
	mem := memory.DefaultAllocator
	builders := make([]array.Builder, len(u.args))
	for i, t := range u.args {
		builders[i] = array.NewBuilder(mem, t)
		defer builders[i].Release()
	}
	rows := 0
	for row, err := range st.Rows() {
		if err != nil {
			return err
		}
		for i, b := range builders {
			if err := appendValue(b, *row.GetValuePtr(i)); err != nil {
				return fmt.Errorf("function %s: %w", u.f.Name, err)
			}
		}
		rows++
	}
	if rows == 0 {
		return nil
	}
	arrs := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrs[i] = b.NewArray()
		defer arrs[i].Release()
	}
	res, err := u.f.Call(ctx, arrs)
	if err != nil {
		return fmt.Errorf("function %s: %w", u.f.Name, err)
	}
	defer res.Release()
	if res.Len() != rows {
		return fmt.Errorf("function %s: returned %d values for %d rows", u.f.Name, res.Len(), rows)
	}
	if !arrow.TypeEqual(res.DataType(), u.ret) {
		return fmt.Errorf("function %s: returned %s, want %s", u.f.Name, res.DataType(), u.ret)
	}
	i := 0
	for row, err := range st.Rows() {
		if err != nil {
			return err
		}
		var v any
		if !res.IsNull(i) {
			v = arrayValue(res, i)
		}
		if err := row.SetResult(v); err != nil {
			return err
		}
		i++
	}
	return nil
}

func appendValue(b array.Builder, v any) error {
	switch bb := b.(type) {
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.Int8Builder:
		x, ok := v.(int8)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.Int16Builder:
		x, ok := v.(int16)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.Int32Builder:
		x, ok := v.(int32)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.Int64Builder:
		x, ok := v.(int64)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.Float32Builder:
		x, ok := v.(float32)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.Float64Builder:
		x, ok := v.(float64)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	case *array.StringBuilder:
		x, ok := v.(string)
		if !ok {
			return typeErr(v)
		}
		bb.Append(x)
	default:
		return fmt.Errorf("unsupported builder %T", b)
	}
	return nil
}

func typeErr(v any) error { return fmt.Errorf("unexpected argument value %T", v) }

func arrayValue(a arrow.Array, i int) any {
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
