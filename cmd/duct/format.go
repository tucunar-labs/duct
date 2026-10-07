package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// rowWriter renders a result set: begin once with the schema, batch per Arrow
// batch, end once at the close.
type rowWriter interface {
	begin(schema *arrow.Schema) error
	batch(rec arrow.RecordBatch) error
	end() error
}

func newWriter(format string, out io.Writer) (rowWriter, error) {
	switch format {
	case "table":
		return &tableWriter{out: out}, nil
	case "csv":
		return &csvWriter{w: csv.NewWriter(out)}, nil
	case "json":
		return &jsonWriter{out: out}, nil
	}
	return nil, fmt.Errorf("unknown format %q (use table, csv or json)", format)
}

func writeStream(st contract.Stream, format string, out io.Writer) error {
	w, err := newWriter(format, out)
	if err != nil {
		return err
	}
	if err := w.begin(st.Schema()); err != nil {
		return err
	}
	for st.Next() {
		if err := w.batch(st.RecordBatch()); err != nil {
			return err
		}
	}
	if err := st.Err(); err != nil {
		return err
	}
	return w.end()
}

func cell(col arrow.Array, i int) (string, bool) {
	if col.IsNull(i) {
		return "", false
	}
	// ValueStr may alias Arrow memory that dies with the batch: copy.
	return strings.Clone(col.ValueStr(i)), true
}

type tableWriter struct {
	out    io.Writer
	header []string
	rows   [][]string
}

func (t *tableWriter) begin(s *arrow.Schema) error {
	for _, f := range s.Fields() {
		t.header = append(t.header, f.Name)
	}
	return nil
}

func (t *tableWriter) batch(rec arrow.RecordBatch) error {
	for i := range int(rec.NumRows()) {
		row := make([]string, rec.NumCols())
		for c := range row {
			v, ok := cell(rec.Column(c), i)
			if !ok {
				v = "NULL"
			}
			row[c] = strings.NewReplacer("\n", `\n`, "\t", " ").Replace(v)
		}
		t.rows = append(t.rows, row)
	}
	return nil
}

func (t *tableWriter) end() error {
	widths := make([]int, len(t.header))
	for i, h := range t.header {
		widths[i] = utf8.RuneCountInString(h)
	}
	for _, r := range t.rows {
		for i, v := range r {
			widths[i] = max(widths[i], utf8.RuneCountInString(v))
		}
	}
	var b bytes.Buffer
	line := func(cells []string) {
		for i, c := range cells {
			if i > 0 {
				b.WriteString(" | ")
			}
			b.WriteString(c)
			if i < len(cells)-1 {
				b.WriteString(strings.Repeat(" ", widths[i]-utf8.RuneCountInString(c)))
			}
		}
		b.WriteByte('\n')
	}
	line(t.header)
	for i, w := range widths {
		if i > 0 {
			b.WriteString("-+-")
		}
		b.WriteString(strings.Repeat("-", w))
	}
	b.WriteByte('\n')
	for _, r := range t.rows {
		line(r)
	}
	plural := "s"
	if len(t.rows) == 1 {
		plural = ""
	}
	fmt.Fprintf(&b, "(%d row%s)\n", len(t.rows), plural)
	_, err := t.out.Write(b.Bytes())
	return err
}

type csvWriter struct{ w *csv.Writer }

func (c *csvWriter) begin(s *arrow.Schema) error {
	names := make([]string, 0, s.NumFields())
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	return c.w.Write(names)
}

func (c *csvWriter) batch(rec arrow.RecordBatch) error {
	row := make([]string, rec.NumCols())
	for i := range int(rec.NumRows()) {
		for col := range row {
			row[col], _ = cell(rec.Column(col), i) // NULL = empty field
		}
		if err := c.w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (c *csvWriter) end() error {
	c.w.Flush()
	return c.w.Error()
}

// jsonWriter emits a JSON array of objects, keys in column order.
type jsonWriter struct {
	out   io.Writer
	names []string
	n     int
}

func (j *jsonWriter) begin(s *arrow.Schema) error {
	for _, f := range s.Fields() {
		j.names = append(j.names, f.Name)
	}
	_, err := io.WriteString(j.out, "[")
	return err
}

func (j *jsonWriter) batch(rec arrow.RecordBatch) error {
	for i := range int(rec.NumRows()) {
		var b bytes.Buffer
		if j.n > 0 {
			b.WriteByte(',')
		}
		b.WriteString("\n  {")
		for c, name := range j.names {
			if c > 0 {
				b.WriteString(", ")
			}
			k, _ := json.Marshal(name)
			v, err := json.Marshal(jsonValue(rec.Column(c), i))
			if err != nil {
				return err
			}
			b.Write(k)
			b.WriteString(": ")
			b.Write(v)
		}
		b.WriteByte('}')
		if _, err := j.out.Write(b.Bytes()); err != nil {
			return err
		}
		j.n++
	}
	return nil
}

func (j *jsonWriter) end() error {
	tail := "]\n"
	if j.n > 0 {
		tail = "\n]\n"
	}
	_, err := io.WriteString(j.out, tail)
	return err
}

func jsonValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	if m, ok := col.(interface{ GetOneForMarshal(int) any }); ok {
		return m.GetOneForMarshal(i)
	}
	return strings.Clone(col.ValueStr(i))
}
