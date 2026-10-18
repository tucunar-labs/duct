package sharing

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// column is one output column: how to select it and its Delta type.
type column struct {
	Name string
	Type string // Delta/Spark type name
	Cast string // "": select as is; else the SQL type to CAST to
}

// deltaType maps an Arrow type to a Delta type; ok is false when it has none
// (the column is then cast, see planColumns).
func deltaType(t arrow.DataType) (string, bool) {
	switch t.ID() {
	case arrow.BOOL:
		return "boolean", true
	case arrow.INT8:
		return "byte", true
	case arrow.INT16, arrow.UINT8:
		return "short", true
	case arrow.INT32, arrow.UINT16:
		return "integer", true
	case arrow.INT64, arrow.UINT32:
		return "long", true
	case arrow.FLOAT32:
		return "float", true
	case arrow.FLOAT64:
		return "double", true
	case arrow.STRING, arrow.LARGE_STRING:
		return "string", true
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "binary", true
	case arrow.DATE32:
		return "date", true
	case arrow.TIMESTAMP:
		return "timestamp", true
	case arrow.DECIMAL128, arrow.DECIMAL256:
		d := t.(arrow.DecimalType)
		return fmt.Sprintf("decimal(%d,%d)", d.GetPrecision(), d.GetScale()), true
	case arrow.DICTIONARY:
		return deltaType(t.(*arrow.DictionaryType).ValueType)
	}
	return "", false
}

// planColumns decides the select list of a copy from the Arrow schema of
// `SELECT * FROM <view>`: types with no Delta equivalent are cast to text (or
// to DECIMAL(20,0) for 64-bit unsigned), never dropped.
func planColumns(schema *arrow.Schema) []column {
	cols := make([]column, len(schema.Fields()))
	for i, f := range schema.Fields() {
		c := column{Name: f.Name}
		switch {
		case f.Type.ID() == arrow.UINT64:
			c.Type, c.Cast = "decimal(20,0)", "DECIMAL(20,0)"
		default:
			if t, ok := deltaType(f.Type); ok {
				c.Type = t
			} else {
				c.Type, c.Cast = "string", "VARCHAR"
			}
		}
		cols[i] = c
	}
	return cols
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func selectList(cols []column) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		if c.Cast == "" {
			parts[i] = quoteIdent(c.Name)
		} else {
			parts[i] = "CAST(" + quoteIdent(c.Name) + " AS " + c.Cast + ") AS " + quoteIdent(c.Name)
		}
	}
	return strings.Join(parts, ", ")
}

// schemaString is the Spark JSON of the table, as the protocol wants it.
func schemaString(cols []column) string {
	type field struct {
		Name     string         `json:"name"`
		Type     string         `json:"type"`
		Nullable bool           `json:"nullable"`
		Metadata map[string]any `json:"metadata"`
	}
	fields := make([]field, len(cols))
	for i, c := range cols {
		fields[i] = field{c.Name, c.Type, true, map[string]any{}}
	}
	b, _ := json.Marshal(map[string]any{"type": "struct", "fields": fields})
	return string(b)
}

// localFile is a Parquet file written to a temporary folder.
type localFile struct {
	Path  string
	Rows  int64
	Size  int64
	Stats string // Delta statistics JSON
}

// writeParquet drains rdr into Parquet files of up to rowsPerFile rows (zstd),
// in dir. It returns the files; with no rows it returns none. observe, if not
// nil, sees every non-empty batch written.
func writeParquet(rdr array.RecordReader, dir string, rowsPerFile int64, observe func(arrow.RecordBatch)) ([]localFile, error) {
	var (
		files []localFile
		cur   *os.File
		w     *pqarrow.FileWriter
		rows  int64
		stats *fileStats
	)
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	closeCur := func() error {
		if w == nil {
			return nil
		}
		err := w.Close() // also closes the file
		w = nil
		if err != nil {
			_ = cur.Close()
			return err
		}
		st, err := os.Stat(cur.Name())
		if err != nil {
			return err
		}
		files = append(files, localFile{Path: cur.Name(), Rows: rows, Size: st.Size(), Stats: stats.JSON()})
		rows = 0
		return nil
	}
	fail := func(err error) ([]localFile, error) {
		if w != nil {
			_ = w.Close()
			_ = cur.Close()
		}
		return nil, err
	}
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if rec.NumRows() == 0 {
			continue
		}
		if w == nil {
			f, err := os.CreateTemp(dir, "part-*.parquet")
			if err != nil {
				return fail(err)
			}
			cur = f
			nw, err := pqarrow.NewFileWriter(rdr.Schema(), f, props, pqarrow.DefaultWriterProps())
			if err != nil {
				_ = f.Close()
				return fail(err)
			}
			w = nw
			stats = newFileStats(rdr.Schema())
		}
		if err := w.Write(rec); err != nil {
			return fail(err)
		}
		stats.add(rec)
		if observe != nil {
			observe(rec)
		}
		rows += rec.NumRows()
		if rows >= rowsPerFile {
			if err := closeCur(); err != nil {
				return fail(err)
			}
		}
	}
	if err := rdr.Err(); err != nil {
		return fail(err)
	}
	if err := closeCur(); err != nil {
		return fail(err)
	}
	return files, nil
}
