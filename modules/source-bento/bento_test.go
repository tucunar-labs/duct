package sourcebento_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
	_ "github.com/alonsoburon/duct/modules/source-bento"
)

func newSource(t *testing.T, opts map[string]any) contract.Source {
	t.Helper()
	info, ok := registry.Lookup(contract.KindSource, "bento")
	if !ok {
		t.Fatal("source/bento not registered")
	}
	m := info.New().(contract.Source)
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Init(context.Background(), contract.Config{"options": opts}); err != nil {
		t.Fatal(err)
	}
	return m
}

// pagesServer serves 3 pages of 2 items: {"items":[{"id","name","score","tags"}]}.
func pagesServer(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := strconv.Atoi(r.URL.Query().Get("page"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"items":[{"id":%d,"name":"n%d","score":%d.5,"tags":["a"]},{"id":%d,"name":"n%d","score":null}]}`,
			p*10+1, p*10+1, p, p*10+2, p*10+2)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestPaginatedREST(t *testing.T) {
	srv := pagesServer(t)
	src := newSource(t, map[string]any{
		"input": map[string]any{"generate": map[string]any{"count": 3, "interval": "", "mapping": `root.page = count("pages")`}},
		"processors": []any{
			map[string]any{"http": map[string]any{"url": srv.URL + `/items?page=${! json("page") }`, "verb": "GET"}},
			map[string]any{"mapping": `root = this.items`},
		},
	})
	var batches int
	var rows int64
	var schema *arrow.Schema
	err := src.Read(context.Background(), func(b contract.Batch) error {
		batches++
		rows += b.NumRows()
		schema = b.Schema()
		if b.NumRows() == 0 {
			t.Error("empty batch emitted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rows != 6 || batches != 1 {
		t.Fatalf("rows=%d batches=%d, want 6 and 1", rows, batches)
	}
	want := map[string]arrow.Type{"id": arrow.INT64, "name": arrow.STRING, "score": arrow.FLOAT64, "tags": arrow.STRING}
	if schema.NumFields() != len(want) {
		t.Fatalf("schema: %v", schema)
	}
	for name, typ := range want {
		idx := schema.FieldIndices(name)
		if len(idx) != 1 || schema.Field(idx[0]).Type.ID() != typ {
			t.Errorf("column %s: %v, want %v", name, schema.Fields(), typ)
		}
	}
}

func TestInitErrors(t *testing.T) {
	info, _ := registry.Lookup(contract.KindSource, "bento")
	for name, opts := range map[string]map[string]any{
		"no input":    {},
		"bad input":   {"input": map[string]any{"nope_not_a_component": map[string]any{}}},
		"bad process": {"input": map[string]any{"generate": map[string]any{"count": 1, "mapping": "root = {}"}}, "processors": "x"},
	} {
		m := info.New()
		if err := m.Init(context.Background(), contract.Config{"options": opts}); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
