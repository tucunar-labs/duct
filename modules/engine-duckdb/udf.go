package engineduckdb

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

var _ contract.ScalarRegistrar = (*Engine)(nil)

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
		st, err := contract.ParseSQLType(t)
		if err != nil {
			return nil, fmt.Errorf("engine-duckdb: function %s: argument %d: %w", f.Name, i+1, err)
		}
		ti, err := typeInfoOf(st)
		if err != nil {
			return nil, fmt.Errorf("engine-duckdb: function %s: argument %d: %w", f.Name, i+1, err)
		}
		u.cfg.InputTypeInfos = append(u.cfg.InputTypeInfos, ti)
		u.args = append(u.args, st.Arrow())
	}
	st, err := contract.ParseSQLType(f.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("engine-duckdb: function %s: return: %w", f.Name, err)
	}
	ti, err := typeInfoOf(st)
	if err != nil {
		return nil, fmt.Errorf("engine-duckdb: function %s: return: %w", f.Name, err)
	}
	u.cfg.ResultTypeInfo = ti
	u.ret = st.Arrow()
	return u, nil
}

// typeInfoOf builds the DuckDB type of a parsed UDF type.
func typeInfoOf(t *contract.SQLType) (duckdb.TypeInfo, error) {
	switch t.Kind {
	case "DECIMAL":
		return duckdb.NewDecimalInfo(t.Width, t.Scale)
	case "LIST":
		el, err := typeInfoOf(t.Elem)
		if err != nil {
			return nil, err
		}
		return duckdb.NewListInfo(el)
	case "STRUCT":
		var ents []duckdb.StructEntry
		for _, f := range t.Fields {
			fi, err := typeInfoOf(f.Type)
			if err != nil {
				return nil, err
			}
			e, err := duckdb.NewStructEntry(fi, f.Name)
			if err != nil {
				return nil, err
			}
			ents = append(ents, e)
		}
		return duckdb.NewStructInfo(ents[0], ents[1:]...)
	}
	kinds := map[string]duckdb.Type{
		"BOOLEAN": duckdb.TYPE_BOOLEAN, "TINYINT": duckdb.TYPE_TINYINT, "SMALLINT": duckdb.TYPE_SMALLINT,
		"INTEGER": duckdb.TYPE_INTEGER, "BIGINT": duckdb.TYPE_BIGINT, "FLOAT": duckdb.TYPE_FLOAT,
		"DOUBLE": duckdb.TYPE_DOUBLE, "VARCHAR": duckdb.TYPE_VARCHAR, "DATE": duckdb.TYPE_DATE,
		"TIMESTAMP": duckdb.TYPE_TIMESTAMP,
	}
	k, ok := kinds[t.Kind]
	if !ok {
		return nil, fmt.Errorf("unsupported type %s", t.Kind)
	}
	return duckdb.NewTypeInfo(k)
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
		if err := row.SetResult(arrayValue(res, i)); err != nil {
			return err
		}
		i++
	}
	return nil
}

// appendValue appends one DuckDB value (as database/sql hands it over) to b.
// Nested NULLs (list elements, struct members) arrive as nil.
func appendValue(b array.Builder, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
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
	case *array.Date32Builder:
		x, ok := v.(time.Time)
		if !ok {
			return typeErr(v)
		}
		bb.Append(arrow.Date32FromTime(x))
	case *array.TimestampBuilder:
		x, ok := v.(time.Time)
		if !ok {
			return typeErr(v)
		}
		bb.Append(arrow.Timestamp(x.UnixMicro()))
	case *array.Decimal128Builder:
		x, ok := v.(duckdb.Decimal)
		if !ok || x.Value == nil {
			return typeErr(v)
		}
		bb.Append(decimal128.FromBigInt(x.Value))
	case *array.ListBuilder:
		x, ok := v.([]any)
		if !ok {
			return typeErr(v)
		}
		bb.Append(true)
		child := bb.ValueBuilder()
		for _, e := range x {
			if err := appendValue(child, e); err != nil {
				return err
			}
		}
	case *array.StructBuilder:
		x, ok := v.(map[string]any)
		if !ok {
			return typeErr(v)
		}
		bb.Append(true)
		st := bb.Type().(*arrow.StructType)
		for i, f := range st.Fields() {
			if err := appendValue(bb.FieldBuilder(i), x[f.Name]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported builder %T", b)
	}
	return nil
}

func typeErr(v any) error { return fmt.Errorf("unexpected argument value %T", v) }

// arrayValue returns element i of a (not NULL) as the Go value DuckDB accepts
// for its type; nested NULLs are nil.
func arrayValue(a arrow.Array, i int) any {
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
		return x.Value(i).ToTime()
	case *array.Timestamp:
		return x.Value(i).ToTime(arrow.Microsecond)
	case *array.Decimal128:
		t := x.DataType().(*arrow.Decimal128Type)
		return duckdb.Decimal{Width: uint8(t.Precision), Scale: uint8(t.Scale), Value: x.Value(i).BigInt()}
	case *array.List:
		start, end := x.ValueOffsets(i)
		out := make([]any, 0, end-start)
		child := x.ListValues()
		for j := start; j < end; j++ {
			out = append(out, arrayValue(child, int(j)))
		}
		return out
	case *array.Struct:
		st := x.DataType().(*arrow.StructType)
		out := make(map[string]any, len(st.Fields()))
		for k, f := range st.Fields() {
			out[f.Name] = arrayValue(x.Field(k), i)
		}
		return out
	}
	return nil
}
