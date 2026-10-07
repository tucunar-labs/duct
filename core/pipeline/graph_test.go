package pipeline

import (
	"reflect"
	"strings"
	"testing"
)

func TestGraphOrderAndPlan(t *testing.T) {
	g, err := NewGraph(map[string][]string{
		"gold.a":    {"silver.x", "silver.y", "bronze.raw"}, // bronze.raw is external
		"silver.x":  {"bronze.raw"},
		"silver.y":  {"silver.x"},
		"gold.b":    {"silver.x"},
		"main.solo": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	all, _ := g.Plan(nil, true)
	if len(all) != 5 {
		t.Fatalf("all = %v", all)
	}
	pos := map[string]int{}
	for i, id := range all {
		pos[id] = i
	}
	for _, e := range [][2]string{{"silver.x", "silver.y"}, {"silver.y", "gold.a"}, {"silver.x", "gold.b"}} {
		if pos[e[0]] > pos[e[1]] {
			t.Errorf("%s must come before %s: %v", e[0], e[1], all)
		}
	}
	up, err := g.Plan([]string{"gold.a"}, true)
	if err != nil || !reflect.DeepEqual(up, []string{"silver.x", "silver.y", "gold.a"}) {
		t.Errorf("upstream plan = %v, %v", up, err)
	}
	only, _ := g.Plan([]string{"gold.a"}, false)
	if !reflect.DeepEqual(only, []string{"gold.a"}) {
		t.Errorf("only plan = %v", only)
	}
	if _, err := g.Plan([]string{"nope.t"}, true); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Errorf("unknown target: %v", err)
	}
}

func TestGraphCycle(t *testing.T) {
	_, err := NewGraph(map[string][]string{"a.x": {"a.y"}, "a.y": {"a.z"}, "a.z": {"a.x"}, "a.ok": nil})
	if err == nil || !strings.Contains(err.Error(), "a.x -> a.y -> a.z -> a.x") {
		t.Fatalf("cycle error = %v", err)
	}
	_, err = NewGraph(map[string][]string{"a.self": {"a.self"}})
	if err == nil || !strings.Contains(err.Error(), "a.self -> a.self") {
		t.Fatalf("self cycle error = %v", err)
	}
}
