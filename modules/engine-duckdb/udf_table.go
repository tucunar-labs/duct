package engineduckdb

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

var _ contract.TableRegistrar = (*Engine)(nil)

// chunkRows is DuckDB's vector size: the most rows a chunk holds.
const chunkRows = 2048

// RegisterTable implements contract.TableRegistrar. The function runs once per
// call site at bind time (the arguments are constants); the rows it returns are
// then handed to DuckDB chunk by chunk.
func (e *Engine) RegisterTable(ctx context.Context, f contract.TableFunction) error {
	db, err := e.handle()
	if err != nil {
		return err
	}
	if f.Name == "" || f.Call == nil || len(f.Columns) == 0 {
		return fmt.Errorf("engine-duckdb: table function needs a name, columns and Call")
	}
	var (
		cfg      duckdb.TableFunctionConfig
		argTypes []arrow.DataType
		cols     []duckdb.ColumnInfo
	)
	for i, t := range f.ArgTypes {
		st, err := contract.ParseSQLType(t)
		if err != nil {
			return fmt.Errorf("engine-duckdb: function %s: argument %d: %w", f.Name, i+1, err)
		}
		ti, err := typeInfoOf(st)
		if err != nil {
			return fmt.Errorf("engine-duckdb: function %s: argument %d: %w", f.Name, i+1, err)
		}
		cfg.Arguments = append(cfg.Arguments, ti)
		argTypes = append(argTypes, st.Arrow())
	}
	var want []arrow.DataType
	for _, c := range f.Columns {
		st, err := contract.ParseSQLType(c.Type)
		if err != nil {
			return fmt.Errorf("engine-duckdb: function %s: column %s: %w", f.Name, c.Name, err)
		}
		ti, err := typeInfoOf(st)
		if err != nil {
			return fmt.Errorf("engine-duckdb: function %s: column %s: %w", f.Name, c.Name, err)
		}
		cols = append(cols, duckdb.ColumnInfo{Name: c.Name, T: ti})
		want = append(want, st.Arrow())
	}
	tf := duckdb.ChunkTableFunction{
		Config: cfg,
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, args ...any) (duckdb.ChunkTableSource, error) {
			arrs := make([]arrow.Array, len(args))
			for i, v := range args {
				b := array.NewBuilder(memory.DefaultAllocator, argTypes[i])
				if err := appendValue(b, v); err != nil {
					b.Release()
					return nil, fmt.Errorf("function %s: %w", f.Name, err)
				}
				arrs[i] = b.NewArray()
				b.Release()
				defer arrs[i].Release()
			}
			batches, err := f.Call(ctx, arrs)
			if err != nil {
				return nil, fmt.Errorf("function %s: %w", f.Name, err)
			}
			src := &tableSource{name: f.Name, cols: cols, batches: batches}
			for _, b := range batches {
				if int(b.NumCols()) != len(want) {
					src.release()
					return nil, fmt.Errorf("function %s: returned %d columns, want %d", f.Name, b.NumCols(), len(want))
				}
				for i, w := range want {
					if !arrow.TypeEqual(b.Column(i).DataType(), w) {
						src.release()
						return nil, fmt.Errorf("function %s: column %s is %s, want %s", f.Name, f.Columns[i].Name, b.Column(i).DataType(), w)
					}
				}
			}
			return src, nil
		},
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := duckdb.RegisterTableUDF(conn, f.Name, tf); err != nil {
		return fmt.Errorf("engine-duckdb: register %s: %w", f.Name, err)
	}
	if f.Close != nil {
		e.mu.Lock()
		e.udfs = append(e.udfs, f.Close)
		e.mu.Unlock()
	}
	return nil
}

type tableSource struct {
	name    string
	cols    []duckdb.ColumnInfo
	batches []arrow.RecordBatch
	bi, row int // next batch and row inside it
}

func (s *tableSource) ColumnInfos() []duckdb.ColumnInfo { return s.cols }
func (s *tableSource) Cardinality() *duckdb.CardinalityInfo {
	var n uint
	for _, b := range s.batches {
		n += uint(b.NumRows())
	}
	return &duckdb.CardinalityInfo{Cardinality: n, Exact: true}
}
func (s *tableSource) Init() {}

func (s *tableSource) release() {
	for _, b := range s.batches {
		b.Release()
	}
	s.batches = nil
}

func (s *tableSource) FillChunk(chunk duckdb.DataChunk) error {
	n := 0
	for n < chunkRows && s.bi < len(s.batches) {
		b := s.batches[s.bi]
		if s.row >= int(b.NumRows()) {
			s.bi++
			s.row = 0
			continue
		}
		for c := range int(b.NumCols()) {
			if err := chunk.SetValue(c, n, arrayValue(b.Column(c), s.row)); err != nil {
				return fmt.Errorf("function %s: %w", s.name, err)
			}
		}
		s.row++
		n++
	}
	if n == 0 {
		s.release()
	}
	return chunk.SetSize(n)
}
