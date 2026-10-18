package sharing

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const stat = `{"numRecords":3,"minValues":{"id":10,"name":"bob","day":"2024-01-10","x":1.5},"maxValues":{"id":20,"name":"carl","day":"2024-01-20","x":2.5},"nullCount":{"id":0,"name":1,"day":0,"x":3,"allnull":3}}`

func jcmp(op, col, ctyp, val, vtyp string) string {
	return `{"op":"` + op + `","children":[{"op":"column","name":"` + col + `","valueType":"` + ctyp + `"},{"op":"literal","value":"` + val + `","valueType":"` + vtyp + `"}]}`
}

func TestPredicateMayMatch(t *testing.T) {
	for _, tc := range []struct {
		name, json string
		legacy     []string
		want       bool
	}{
		{"eq inside", jcmp("equal", "id", "int", "15", "int"), nil, true},
		{"eq below", jcmp("equal", "id", "int", "9", "int"), nil, false},
		{"eq above", jcmp("equal", "id", "long", "21", "long"), nil, false},
		{"eq at the edge", jcmp("equal", "id", "int", "20", "int"), nil, true},
		{"lt at min", jcmp("lessThan", "id", "int", "10", "int"), nil, false},
		{"lt above min", jcmp("lessThan", "id", "int", "11", "int"), nil, true},
		{"le at min", jcmp("lessThanOrEqual", "id", "int", "10", "int"), nil, true},
		{"gt at max", jcmp("greaterThan", "id", "int", "20", "int"), nil, false},
		{"ge at max", jcmp("greaterThanOrEqual", "id", "int", "20", "int"), nil, true},
		{"literal first is flipped", `{"op":"lessThan","children":[{"op":"literal","value":"19","valueType":"int"},{"op":"column","name":"id","valueType":"int"}]}`, nil, true},
		{"literal first, pruned", `{"op":"lessThan","children":[{"op":"literal","value":"25","valueType":"int"},{"op":"column","name":"id","valueType":"int"}]}`, nil, false},
		{"string below", jcmp("lessThan", "name", "string", "a", "string"), nil, false},
		{"string inside", jcmp("equal", "name", "string", "bz", "string"), nil, true},
		{"date out", jcmp("greaterThan", "day", "date", "2024-02-01", "date"), nil, false},
		{"date in", jcmp("equal", "day", "date", "2024-01-15", "date"), nil, true},
		{"double out", jcmp("greaterThan", "x", "double", "2.5", "double"), nil, false},
		{"int literal on a double column", jcmp("lessThan", "x", "double", "1", "int"), nil, false},
		{"is null, no nulls", `{"op":"isNull","children":[{"op":"column","name":"id","valueType":"int"}]}`, nil, false},
		{"is null, some", `{"op":"isNull","children":[{"op":"column","name":"name","valueType":"string"}]}`, nil, true},
		{"not is null, all null", `{"op":"not","children":[{"op":"isNull","children":[{"op":"column","name":"allnull","valueType":"int"}]}]}`, nil, false},
		{"and prunes if one side does", `{"op":"and","children":[` + jcmp("equal", "id", "int", "15", "int") + `,` + jcmp("equal", "id", "int", "99", "int") + `]}`, nil, false},
		{"or keeps if one side may", `{"op":"or","children":[` + jcmp("equal", "id", "int", "15", "int") + `,` + jcmp("equal", "id", "int", "99", "int") + `]}`, nil, true},
		{"or prunes if both do", `{"op":"or","children":[` + jcmp("equal", "id", "int", "5", "int") + `,` + jcmp("equal", "id", "int", "99", "int") + `]}`, nil, false},
		// NOT of a pruned comparison must not prune: NULL rows satisfy neither.
		{"not of a false comparison", `{"op":"not","children":[` + jcmp("equal", "id", "int", "99", "int") + `]}`, nil, true},
		{"unknown column", jcmp("equal", "nope", "int", "1", "int"), nil, true},
		{"column without stats", jcmp("equal", "allnull", "int", "1", "int"), nil, true},
		{"type mismatch: text literal on a number", jcmp("equal", "id", "string", "abc", "string"), nil, true},
		{"timestamps are not pruned", jcmp("greaterThan", "id", "timestamp", "2030-01-01T00:00:00Z", "timestamp"), nil, true},
		{"garbage", `{"op":`, nil, true},
		{"unknown op", `{"op":"startsWith","children":[]}`, nil, true},
		{"absurd exponent", jcmp("equal", "id", "long", "1e999999999", "long"), nil, true},
		{"legacy eq out", "", []string{"id = 5"}, false},
		{"legacy eq in", "", []string{"id = 15"}, true},
		{"legacy reversed", "", []string{"25 < id"}, false},
		{"legacy string", "", []string{"name >= 'zed'"}, false},
		{"legacy ge", "", []string{"id >= 20"}, true},
		{"legacy ne all equal", "", []string{"id <> 10"}, true},
		{"legacy is null on a column without nulls", "", []string{"id IS NULL"}, false},
		{"legacy is not null", "", []string{"name IS NOT NULL"}, true},
		{"legacy ANDed", "", []string{"id >= 10", "id < 10"}, false},
		{"legacy unparsable", "", []string{"id + 1 > 3", "lower(name) = 'x'"}, true},
		{"legacy number on a string column", "", []string{"name = 5"}, true},
		{"none", "", nil, true},
	} {
		p := ParseHints(tc.json, tc.legacy)
		if got := p.MayMatch(stat); got != tc.want {
			t.Errorf("%s: MayMatch = %v, want %v", tc.name, got, tc.want)
		}
	}
	// No statistics, bad statistics or a nil predicate keep every file.
	p := ParseHints(jcmp("equal", "id", "int", "99", "int"), nil)
	for _, s := range []string{"", "{", `{"numRecords":1}`} {
		if !p.MayMatch(s) {
			t.Errorf("stats %q pruned a file", s)
		}
	}
	var nilp *Predicate
	if !nilp.MayMatch(stat) {
		t.Error("nil predicate pruned")
	}
}

func TestFileStats(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "long", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{5, 0, 9}, []bool{true, false, true})
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"b", "a", ""}, []bool{true, true, false})
	b.Field(2).(*array.Date32Builder).AppendValues([]arrow.Date32{19723, 19724, 19725}, nil)
	b.Field(3).(*array.Float64Builder).AppendValues([]float64{1, 2, 0}, []bool{true, true, false})
	b.Field(4).(*array.StringBuilder).AppendValues([]string{"ok", string(make([]byte, 100)), "z"}, nil)
	b.Field(5).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	fs := newFileStats(sc)
	fs.add(rec)
	p := ParseHints(jcmp("greaterThan", "id", "long", "9", "long"), nil)
	if p.MayMatch(fs.JSON()) {
		t.Errorf("id > 9 kept a file whose max is 9: %s", fs.JSON())
	}
	for _, h := range []string{jcmp("equal", "id", "long", "5", "long"), jcmp("equal", "s", "string", "a", "string"), jcmp("equal", "d", "date", "2024-01-01", "date"), jcmp("equal", "x", "double", "2", "double"),
		jcmp("equal", "long", "string", "zzz", "string"), jcmp("equal", "ts", "timestamp", "2030-01-01T00:00:00Z", "timestamp")} {
		if !ParseHints(h, nil).MayMatch(fs.JSON()) {
			t.Errorf("sound hint pruned the file: %s\n%s", h, fs.JSON())
		}
	}
	if ParseHints(jcmp("equal", "s", "string", "zz", "string"), nil).MayMatch(fs.JSON()) {
		t.Errorf("s = 'zz' kept: %s", fs.JSON())
	}
	if ParseHints(jcmp("lessThan", "d", "date", "2024-01-01", "date"), nil).MayMatch(fs.JSON()) {
		t.Errorf("d < min kept: %s", fs.JSON())
	}
	if !ParseHints("", []string{"id IS NULL"}).MayMatch(fs.JSON()) {
		t.Errorf("id IS NULL pruned a file with a null id: %s", fs.JSON())
	}
}

func TestChangesSQLRejectsReservedColumns(t *testing.T) {
	def := TableDef{Relation: "gold.t"}
	if _, err := changesSQL(def, []column{{Name: "rowid", Type: "long"}}, "", nil, 1, 2); err == nil {
		t.Error("a column named like the feed's own was accepted")
	}
	q, err := changesSQL(def, []column{{Name: "id", Type: "long"}, {Name: "u", Type: "decimal(20,0)", Cast: "DECIMAL(20,0)"}}, "id > 1", map[string]string{"id": "id + 1"}, 3, 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{`table_changes('t', 3, 5)`, `lake."gold"`, `(id + 1`, `CAST(b."u" AS DECIMAL(20,0)) AS "u"`} {
		if !strings.Contains(q, w) {
			t.Errorf("query lacks %q:\n%s", w, q)
		}
	}
}
