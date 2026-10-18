package sharing

import (
	"encoding/json"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// maxStatString is the longest string kept as a min/max statistic; a file with
// a longer value in a column simply has no statistics for it (no pruning).
const maxStatString = 64

// colStats accumulates the statistics of one column of one file.
type colStats struct {
	nulls   int64
	has     bool // min/max hold a value
	invalid bool // min/max cannot be trusted (unsupported type, NaN, long string)
	// Exactly one of the pairs is used, by the kind of the column.
	imin, imax int64
	fmin, fmax float64
	smin, smax string
	kind       statKind
}

type statKind int

const (
	kindNone statKind = iota
	kindInt
	kindFloat
	kindString
	kindDate
)

// fileStats accumulates the Delta per-file statistics of a Parquet file.
type fileStats struct {
	names []string
	cols  []*colStats
	rows  int64
}

func newFileStats(schema *arrow.Schema) *fileStats {
	fs := &fileStats{}
	for _, f := range schema.Fields() {
		cs := &colStats{}
		switch f.Type.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32:
			cs.kind = kindInt
		case arrow.FLOAT64: // float32 would not round-trip against a client's double literal
			cs.kind = kindFloat
		case arrow.STRING, arrow.LARGE_STRING:
			cs.kind = kindString
		case arrow.DATE32:
			cs.kind = kindDate
		default:
			cs.invalid = true
		}
		fs.names = append(fs.names, f.Name)
		fs.cols = append(fs.cols, cs)
	}
	return fs
}

// add folds one batch in.
func (fs *fileStats) add(rec arrow.RecordBatch) {
	fs.rows += rec.NumRows()
	for i, cs := range fs.cols {
		col := rec.Column(i)
		cs.nulls += int64(col.NullN())
		if cs.invalid || col.NullN() == int(rec.NumRows()) {
			continue
		}
		for r := range int(rec.NumRows()) {
			if col.IsNull(r) {
				continue
			}
			cs.fold(col, r)
			if cs.invalid {
				break
			}
		}
	}
}

func (cs *colStats) fold(col arrow.Array, r int) {
	switch cs.kind {
	case kindInt:
		var v int64
		switch a := col.(type) {
		case *array.Int8:
			v = int64(a.Value(r))
		case *array.Int16:
			v = int64(a.Value(r))
		case *array.Int32:
			v = int64(a.Value(r))
		case *array.Int64:
			v = a.Value(r)
		case *array.Uint8:
			v = int64(a.Value(r))
		case *array.Uint16:
			v = int64(a.Value(r))
		case *array.Uint32:
			v = int64(a.Value(r))
		default:
			cs.invalid = true
			return
		}
		if !cs.has {
			cs.imin, cs.imax, cs.has = v, v, true
			return
		}
		cs.imin, cs.imax = min(cs.imin, v), max(cs.imax, v)
	case kindFloat:
		a, ok := col.(*array.Float64)
		if !ok {
			cs.invalid = true
			return
		}
		v := a.Value(r)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			cs.invalid = true
			return
		}
		if !cs.has {
			cs.fmin, cs.fmax, cs.has = v, v, true
			return
		}
		cs.fmin, cs.fmax = min(cs.fmin, v), max(cs.fmax, v)
	case kindString:
		var v string
		switch a := col.(type) {
		case *array.String:
			v = a.Value(r)
		case *array.LargeString:
			v = a.Value(r)
		default:
			cs.invalid = true
			return
		}
		if len(v) > maxStatString {
			cs.invalid = true
			return
		}
		cs.foldString(v)
	case kindDate:
		a, ok := col.(*array.Date32)
		if !ok {
			cs.invalid = true
			return
		}
		cs.foldString(a.Value(r).FormattedString())
	}
}

// foldString keeps the extremes. The value may point into the Arrow buffer,
// which its owner reuses: a kept string is always a copy.
func (cs *colStats) foldString(v string) {
	switch {
	case !cs.has:
		cs.smin, cs.smax, cs.has = strings.Clone(v), strings.Clone(v), true
	case v < cs.smin:
		cs.smin = strings.Clone(v)
	case v > cs.smax:
		cs.smax = strings.Clone(v)
	}
}

// JSON is the Delta statistics string of the file.
func (fs *fileStats) JSON() string {
	mins, maxs, nulls := map[string]any{}, map[string]any{}, map[string]int64{}
	for i, cs := range fs.cols {
		nulls[fs.names[i]] = cs.nulls
		if cs.invalid || !cs.has {
			continue
		}
		switch cs.kind {
		case kindInt:
			mins[fs.names[i]], maxs[fs.names[i]] = cs.imin, cs.imax
		case kindFloat:
			mins[fs.names[i]], maxs[fs.names[i]] = cs.fmin, cs.fmax
		case kindString, kindDate:
			mins[fs.names[i]], maxs[fs.names[i]] = cs.smin, cs.smax
		}
	}
	b, _ := json.Marshal(map[string]any{"numRecords": fs.rows, "minValues": mins, "maxValues": maxs, "nullCount": nulls})
	return string(b)
}
