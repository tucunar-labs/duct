package ingest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestParseDefaultsAndValidation(t *testing.T) {
	c, err := Parse([]byte("name: customers\nsource: files\noptions: {path: a.csv}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Target != "bronze.customers" || c.Mode != ModeFull {
		t.Errorf("defaults: %+v", c)
	}
	bad := map[string]string{
		"empty":               "",
		"no source":           "name: a\n",
		"bad name":            "name: Bad-Name\nsource: files\n",
		"target outside":      "name: a\nsource: files\ntarget: silver.a\n",
		"unknown key":         "name: a\nsource: files\nfrequency: daily\n",
		"bad mode":            "name: a\nsource: files\nmode: upsert\n",
		"incremental no wm":   "name: a\nsource: files\nmode: incremental\n",
		"watermark with full": "name: a\nsource: files\nwatermark: id\n",
		"watermark injection": "name: a\nsource: files\nmode: incremental\nwatermark: 'id; drop'\n",
	}
	for name, y := range bad {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestLoadSortedAndUnique(t *testing.T) {
	root := t.TempDir()
	if cs, err := Load(root); err != nil || cs != nil {
		t.Fatalf("missing dir: %v %v", cs, err)
	}
	dir := filepath.Join(root, Dir)
	_ = os.MkdirAll(dir, 0o750)
	w := func(file, body string) {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	w("b.yaml", "name: b\nsource: files\n")
	w("a.yml", "name: a\nsource: odbc\n")
	w("notes.txt", "ignored")
	cs, err := Load(root)
	if err != nil || len(cs) != 2 || cs[0].Name != "a" || cs[1].Name != "b" {
		t.Fatalf("got %+v %v", cs, err)
	}
	w("c.yaml", "name: a\nsource: files\n")
	if _, err := Load(root); err == nil || !strings.Contains(err.Error(), "already defined") {
		t.Errorf("duplicate name: %v", err)
	}
}

// The JSON Schema in specs/ and the Go struct must list the same keys.
func TestSchemaMatchesConfig(t *testing.T) {
	data, err := os.ReadFile("../../specs/ingest.schema.json")
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
