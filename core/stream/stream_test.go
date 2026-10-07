package stream

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestParseDefaults(t *testing.T) {
	c, err := Parse([]byte("name: sensors\ntopic: planta.sensores\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Target != "bronze.sensors" || c.Format != "json" || c.Batch.MaxRows != DefaultMaxRows ||
		c.Batch.MaxInterval != time.Second || c.Group != "duct-stream-sensors" || c.Start != "earliest" || c.Window != 0 {
		t.Fatalf("config = %+v", c)
	}
	c, err = Parse([]byte("name: s\ntopic: t\ntarget: bronze.lecturas\nbatch: {max_rows: 10, max_interval: 250ms}\nwindow: 10m\nstart: latest\ninline_rows: 500\n"))
	if err != nil || c.Batch.MaxInterval != 250*time.Millisecond || c.Window != 10*time.Minute || c.Table() != "lecturas" || c.InlineRows != 500 {
		t.Fatalf("config = %+v err %v", c, err)
	}
}

func TestParseErrors(t *testing.T) {
	for name, y := range map[string]string{
		"no topic":   "name: s\n",
		"bad target": "name: s\ntopic: t\ntarget: silver.x\n",
		"format":     "name: s\ntopic: t\nformat: avro\n",
		"unknown":    "name: s\ntopic: t\nbogus: 1\n",
		"start":      "name: s\ntopic: t\nstart: middle\n",
		"negative":   "name: s\ntopic: t\nbatch: {max_rows: -1}\n",
		"name":       "name: Bad-Name\ntopic: t\n",
	} {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestLoadRejectsDuplicates(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, Dir)
	_ = os.MkdirAll(dir, 0o750)
	write := func(f, body string) {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("a.yaml", "name: a\ntopic: t1\n")
	write("b.yaml", "name: b\ntopic: t2\n")
	cfgs, err := Load(root)
	if err != nil || len(cfgs) != 2 || cfgs[0].Name != "a" {
		t.Fatalf("cfgs = %v err %v", cfgs, err)
	}
	write("c.yaml", "name: c\ntopic: t1\n")
	if _, err := Load(root); err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("duplicate topic: %v", err)
	}
	if got, err := Load(t.TempDir()); err != nil || got != nil {
		t.Fatalf("missing dir: %v %v", got, err)
	}
}

func rec(off int64, v string) Record {
	return Record{Topic: "t", Partition: 2, Offset: off, Value: []byte(v), Key: []byte("k"), Timestamp: time.UnixMicro(1_700_000_000_000_000)}
}

func TestDecodeTypes(t *testing.T) {
	d, err := Decode([]Record{
		rec(0, `{"id":"s1","n":1,"f":1.5,"mix":2,"ok":true,"ts":"2026-10-07T12:00:00Z","nest":{"a":[1]},"t":"x","only_null":null}`),
		rec(1, `{"id":"s2","n":2,"f":2,"mix":2.5,"ok":false,"ts":"2026-10-07T12:00:01.25Z","nest":[],"t":"2026-10-07T12:00:00Z","_kafka_offset":9,"_ingested_at":1}`),
		rec(2, `not json`),
		rec(3, `"scalar"`),
		rec(4, `  `),
		rec(5, `{"a":1} trailing`),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Rec.Release()
	if len(d.Rejects) != 4 {
		t.Fatalf("rejects = %v", d.Rejects)
	}
	got := map[string]string{}
	for _, f := range d.Rec.Schema().Fields() {
		got[f.Name] = f.Type.String()
	}
	want := map[string]string{"id": "utf8", "n": "int64", "f": "float64", "mix": "float64", "ok": "bool",
		"ts": "timestamp[us, tz=UTC]", "nest": "utf8", "t": "utf8", // t mixes text and a timestamp: text
		ColPartition: "int32", ColOffset: "int64", ColTimestamp: "timestamp[us, tz=UTC]", ColKey: "utf8"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("column %s: %q want %q", k, got[k], v)
		}
	}
	if _, has := got["only_null"]; has {
		t.Error("an all-null field must not get a column")
	}
	if len(got) != len(want) {
		t.Errorf("columns = %v", got)
	}
	if !d.JSONCols["nest"] || d.JSONCols["t"] {
		t.Errorf("JSONCols = %v", d.JSONCols)
	}
	if d.Rec.NumRows() != 2 {
		t.Fatalf("rows = %d", d.Rec.NumRows())
	}
	// Reserved payload fields never overwrite the metadata.
	idx := d.Rec.Schema().FieldIndices(ColOffset)[0]
	if v := d.Rec.Column(idx).(*array.Int64).Value(1); v != 1 {
		t.Errorf("_kafka_offset = %d", v)
	}
	ts := d.Rec.Column(d.Rec.Schema().FieldIndices("ts")[0]).(*array.Timestamp)
	if ts.Value(1)-ts.Value(0) != arrow.Timestamp(1_250_000) {
		t.Errorf("timestamps = %v %v", ts.Value(0), ts.Value(1))
	}
}

func TestDecodeKeepsFieldOrder(t *testing.T) {
	d, err := Decode([]Record{rec(0, `{"zeta":1,"alfa":2}`), rec(1, `{"mid":3,"alfa":4}`)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Rec.Release()
	var names []string
	for _, f := range d.Rec.Schema().Fields() {
		names = append(names, f.Name)
	}
	if strings.Join(names[:3], ",") != "zeta,alfa,mid" {
		t.Fatalf("columns = %v", names)
	}
}

func TestDecodeAllRejected(t *testing.T) {
	d, err := Decode([]Record{rec(0, `x`)}, nil)
	if err != nil || d.Rec != nil || len(d.Rejects) != 1 {
		t.Fatalf("%+v %v", d, err)
	}
}

// The JSON Schema in specs/ and the Go struct must list the same keys.
func TestSchemaMatchesConfig(t *testing.T) {
	data, err := os.ReadFile("../../specs/stream.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	var fromSchema, fromGo []string
	for k := range schema.Properties {
		fromSchema = append(fromSchema, k)
	}
	typ := reflect.TypeOf(Config{})
	for i := range typ.NumField() {
		if tag := typ.Field(i).Tag.Get("yaml"); tag != "" && tag != "-" {
			fromGo = append(fromGo, tag)
		}
	}
	sort.Strings(fromSchema)
	sort.Strings(fromGo)
	if !reflect.DeepEqual(fromSchema, fromGo) {
		t.Errorf("schema keys %v != Config keys %v", fromSchema, fromGo)
	}
}
