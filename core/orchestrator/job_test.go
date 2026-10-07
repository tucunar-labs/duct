package orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

const goodJob = `
name: nightly
schedule: "*/5 * * * *"
timezone: America/Santiago
triggers:
  on_snapshot: [bronze.orders, customers]
  on_file: data/in/*.csv
tasks:
  - id: load
    type: ingest
    params: {names: [orders]}
    retries: 2
    backoff: 5s
    timeout: 10m
  - id: build
    type: pipeline
    depends_on: [load]
  - id: ping
    type: shell
    depends_on: [build]
    params: {command: [echo, hi]}
alerts:
  on_failure:
    - channel: webhook
      config: {url: "http://x"}
`

func TestParseAndOrder(t *testing.T) {
	j, err := Parse([]byte(goodJob))
	if err != nil {
		t.Fatal(err)
	}
	ord, err := j.Order()
	if err != nil || !reflect.DeepEqual(ord, []string{"load", "build", "ping"}) {
		t.Fatalf("order %v %v", ord, err)
	}
	if got := j.SnapshotTables(); !reflect.DeepEqual(got, []string{"bronze.orders", "main.customers"}) {
		t.Errorf("tables %v", got)
	}
	if j.Tasks[0].MaxAttempts() != 3 || time.Duration(j.Tasks[0].Timeout) != 10*time.Minute {
		t.Errorf("task %+v", j.Tasks[0])
	}
}

func TestDAGOrderIsDependencyFirst(t *testing.T) {
	j := Job{Name: "j", Tasks: []Task{
		{ID: "c", Type: "pipeline", DependsOn: []string{"a", "b"}},
		{ID: "b", Type: "pipeline", DependsOn: []string{"a"}},
		{ID: "a", Type: "pipeline"},
	}}
	ord, err := j.Order()
	if err != nil || !reflect.DeepEqual(ord, []string{"a", "b", "c"}) {
		t.Fatalf("%v %v", ord, err)
	}
}

func TestValidationErrors(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, want string }{
		"cycle":        {"name: j\ntasks:\n - {id: a, type: pipeline, depends_on: [b]}\n - {id: b, type: pipeline, depends_on: [a]}\n", "cycle"},
		"unknown dep":  {"name: j\ntasks:\n - {id: a, type: pipeline, depends_on: [zz]}\n", "not a task"},
		"dup task":     {"name: j\ntasks:\n - {id: a, type: pipeline}\n - {id: a, type: pipeline}\n", "twice"},
		"bad type":     {"name: j\ntasks:\n - {id: a, type: sql}\n", "type"},
		"shell string": {"name: j\ntasks:\n - {id: a, type: shell, params: {command: \"echo hi\"}}\n", "list"},
		"bad cron":     {"name: j\nschedule: \"61 * * * *\"\ntasks:\n - {id: a, type: pipeline}\n", "schedule"},
		"bad tz":       {"name: j\nschedule: \"@daily\"\ntimezone: Mars/Base\ntasks:\n - {id: a, type: pipeline}\n", "timezone"},
		"no tasks":     {"name: j\n", "tasks"},
		"unknown key":  {"name: j\nfoo: 1\ntasks:\n - {id: a, type: pipeline}\n", "foo"},
		"bad name":     {"name: Bad-Name\ntasks:\n - {id: a, type: pipeline}\n", "name"},
		"glob escape":  {"name: j\ntriggers: {on_file: \"../x/*\"}\ntasks:\n - {id: a, type: pipeline}\n", "inside"},
		"python":       {"name: j\ntasks:\n - {id: a, type: python}\n", "script"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRetryDelayBacksOffAndCaps(t *testing.T) {
	tk := Task{Backoff: Duration(time.Second), MaxBackoff: Duration(5 * time.Second)}
	var got []time.Duration
	for n := 1; n <= 5; n++ {
		got = append(got, tk.RetryDelay(n))
	}
	want := []time.Duration{1, 2, 4, 5, 5}
	for i := range want {
		if got[i] != want[i]*time.Second {
			t.Fatalf("delays %v", got)
		}
	}
	if (Task{}).RetryDelay(1) != DefaultBackoff {
		t.Error("default backoff")
	}
}

func TestLoadSortsAndRejectsDuplicates(t *testing.T) {
	root := t.TempDir()
	w := func(f, body string) {
		p := filepath.Join(root, Dir, f)
		_ = os.MkdirAll(filepath.Dir(p), 0o750)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if js, err := Load(root); err != nil || js != nil {
		t.Fatalf("no folder: %v %v", js, err)
	}
	w("b.yaml", "name: b\ntasks:\n - {id: a, type: pipeline}\n")
	w("a.yml", "name: a\ntasks:\n - {id: a, type: pipeline}\n")
	js, err := Load(root)
	if err != nil || len(js) != 2 || js[0].Name != "a" {
		t.Fatalf("%v %v", js, err)
	}
	w("c.yaml", "name: a\ntasks:\n - {id: a, type: pipeline}\n")
	if _, err := Load(root); err == nil || !strings.Contains(err.Error(), "already defined") {
		t.Errorf("dup: %v", err)
	}
}

// The JSON Schema in specs/ and the Go structs must list the same keys.
func TestSchemaMatchesStructs(t *testing.T) {
	data, err := os.ReadFile("../../specs/jobs.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var s struct {
		Properties map[string]struct {
			Properties map[string]any `json:"properties"`
			Items      struct {
				Properties map[string]any `json:"properties"`
			} `json:"items"`
		} `json:"properties"`
		Defs struct {
			Task struct {
				Properties map[string]any `json:"properties"`
			} `json:"task"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	keys := func(m map[string]any) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	tags := func(v any) []string {
		var out []string
		typ := reflect.TypeOf(v)
		for i := range typ.NumField() {
			if tag := typ.Field(i).Tag.Get("yaml"); tag != "" && tag != "-" {
				out = append(out, tag)
			}
		}
		sort.Strings(out)
		return out
	}
	top := map[string]any{}
	for k := range s.Properties {
		top[k] = nil
	}
	for _, c := range []struct {
		name      string
		schema    []string
		goStruct  any
		goFromTop bool
	}{
		{"job", keys(top), Job{}, false},
		{"triggers", keys(s.Properties["triggers"].Properties), Triggers{}, false},
		{"task", keys(s.Defs.Task.Properties), Task{}, false},
		{"alerts", keys(s.Properties["alerts"].Properties), Alerts{}, false},
	} {
		if g := tags(c.goStruct); !reflect.DeepEqual(c.schema, g) {
			t.Errorf("%s: schema keys %v != struct keys %v", c.name, c.schema, g)
		}
	}
}
