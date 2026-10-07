package servehttp

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// lateError wraps an error that happened after the response started.
type lateError struct{ err error }

func (e *lateError) Error() string { return e.err.Error() }
func (e *lateError) Unwrap() error { return e.err }

type encoder struct {
	contentType string
	write       func(w io.Writer, st contract.Stream) (rows int64, err error)
}

func lookupFormat(name string) (encoder, bool) {
	switch strings.ToLower(name) {
	case "json":
		return encoder{"application/json; charset=utf-8", writeJSON}, true
	case "jsoneachrow", "ndjson":
		return encoder{"application/x-ndjson; charset=utf-8", writeJSONEachRow}, true
	case "csv":
		return encoder{"text/csv; charset=utf-8", func(w io.Writer, st contract.Stream) (int64, error) { return writeCSV(w, st, false) }}, true
	case "csvwithnames":
		return encoder{"text/csv; charset=utf-8", func(w io.Writer, st contract.Stream) (int64, error) { return writeCSV(w, st, true) }}, true
	case "arrowstream", "arrow":
		return encoder{"application/vnd.apache.arrow.stream", writeArrow}, true
	case "parquet":
		return encoder{"application/vnd.apache.parquet", writeParquet}, true
	}
	return encoder{}, false
}

// startedWriter sets the headers and status on the first byte, so an error
// before any output can still be reported with a proper status.
type startedWriter struct {
	w       http.ResponseWriter
	ct      string
	started bool
}

func (s *startedWriter) Write(p []byte) (int, error) {
	if !s.started {
		s.started = true
		s.w.Header().Set("Content-Type", s.ct)
		s.w.WriteHeader(http.StatusOK)
	}
	return s.w.Write(p)
}

// writeResult streams st in the encoder's format and returns the row count.
func writeResult(w http.ResponseWriter, st contract.Stream, enc encoder) (int64, error) {
	sw := &startedWriter{w: w, ct: enc.contentType}
	bw := bufio.NewWriterSize(sw, 32<<10)
	rows, err := enc.write(bw, st)
	if err == nil {
		err = bw.Flush()
	}
	if err == nil {
		err = st.Err()
	}
	if err != nil {
		if sw.started || bw.Buffered() == 0 && sw.started {
			return rows, &lateError{err}
		}
		return rows, err
	}
	if !sw.started { // empty body formats (should not happen) still need a status
		_, _ = sw.Write(nil)
	}
	return rows, nil
}

func typeName(t arrow.DataType) string { return t.String() }

func cell(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	v := col.GetOneForMarshal(i)
	switch f := v.(type) {
	case float64:
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
	case float32:
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return nil
		}
	}
	return v
}

func writeJSON(w io.Writer, st contract.Stream) (int64, error) {
	var meta []map[string]string
	for _, f := range st.Schema().Fields() {
		meta = append(meta, map[string]string{"name": f.Name, "type": typeName(f.Type)})
	}
	mb, err := json.Marshal(meta)
	if err != nil {
		return 0, err
	}
	if _, err := io.WriteString(w, `{"meta":`+string(mb)+`,"data":[`); err != nil {
		return 0, err
	}
	var n int64
	names := st.Schema().Fields()
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			if n > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return n, err
				}
			}
			row := make(map[string]any, len(names))
			for c, f := range names {
				row[f.Name] = cell(rec.Column(c), i)
			}
			b, err := json.Marshal(row)
			if err != nil {
				return n, err
			}
			if _, err := w.Write(b); err != nil {
				return n, err
			}
			n++
		}
	}
	if err := st.Err(); err != nil {
		return n, err
	}
	_, err = io.WriteString(w, `],"rows":`+strconv.FormatInt(n, 10)+"}\n")
	return n, err
}

func writeJSONEachRow(w io.Writer, st contract.Stream) (int64, error) {
	var n int64
	fields := st.Schema().Fields()
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make(map[string]any, len(fields))
			for c, f := range fields {
				row[f.Name] = cell(rec.Column(c), i)
			}
			b, err := json.Marshal(row)
			if err != nil {
				return n, err
			}
			if _, err := w.Write(append(b, '\n')); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, st.Err()
}

func writeCSV(w io.Writer, st contract.Stream, header bool) (int64, error) {
	cw := csv.NewWriter(w)
	fields := st.Schema().Fields()
	if header {
		names := make([]string, len(fields))
		for i, f := range fields {
			names[i] = f.Name
		}
		if err := cw.Write(names); err != nil {
			return 0, err
		}
	}
	var n int64
	line := make([]string, len(fields))
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			for c := range line {
				if col := rec.Column(c); col.IsNull(i) {
					line[c] = ""
				} else {
					line[c] = col.ValueStr(i)
				}
			}
			if err := cw.Write(line); err != nil {
				return n, err
			}
			n++
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return n, err
	}
	return n, st.Err()
}

func writeArrow(w io.Writer, st contract.Stream) (int64, error) {
	iw := ipc.NewWriter(w, ipc.WithSchema(st.Schema()))
	var n int64
	for st.Next() {
		rec := st.RecordBatch()
		if err := iw.Write(rec); err != nil {
			_ = iw.Close()
			return n, err
		}
		n += rec.NumRows()
	}
	if err := st.Err(); err != nil {
		_ = iw.Close()
		return n, err
	}
	return n, iw.Close()
}

func writeParquet(w io.Writer, st contract.Stream) (int64, error) {
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	fw, err := pqarrow.NewFileWriter(st.Schema(), w, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return 0, err
	}
	var n int64
	for st.Next() {
		rec := st.RecordBatch()
		if err := fw.Write(rec); err != nil {
			_ = fw.Close()
			return n, err
		}
		n += rec.NumRows()
	}
	if err := st.Err(); err != nil {
		return n, errors.Join(err, fw.Close())
	}
	return n, fw.Close()
}

var _ = array.NewRecordReader
