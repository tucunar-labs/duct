package registry_test

import (
	"context"
	"sync"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

type stub struct{ name string }

func (s stub) Name() string                              { return s.name }
func (stub) Init(context.Context, contract.Config) error { return nil }
func (stub) Close() error                                { return nil }

func info(kind contract.Kind, name string) registry.Info {
	return registry.Info{Kind: kind, Name: name, New: func() contract.Module { return stub{name} }}
}

func TestRegisterLookupList(t *testing.T) {
	registry.Reset()
	t.Cleanup(registry.Reset)
	registry.Register(info(contract.KindSink, "b"))
	registry.Register(info(contract.KindSource, "z"))
	registry.Register(info(contract.KindSink, "a"))

	got, ok := registry.Lookup(contract.KindSink, "a")
	if !ok || got.New().Name() != "a" {
		t.Fatalf("Lookup sink/a = %v, %v", got, ok)
	}
	if _, ok := registry.Lookup(contract.KindSource, "a"); ok {
		t.Fatal("Lookup must be scoped by kind")
	}
	all := registry.List("")
	if len(all) != 3 || all[0].Name != "a" || all[1].Name != "b" || all[2].Name != "z" {
		t.Fatalf("List order wrong: %+v", all)
	}
	if n := len(registry.List(contract.KindSink)); n != 2 {
		t.Fatalf("List(sink) = %d, want 2", n)
	}
}

func TestRegisterPanics(t *testing.T) {
	registry.Reset()
	t.Cleanup(registry.Reset)
	registry.Register(info(contract.KindSink, "a"))
	for name, i := range map[string]registry.Info{
		"duplicate": info(contract.KindSink, "a"),
		"no name":   info(contract.KindSink, ""),
		"no kind":   info("", "x"),
		"nil New":   {Kind: contract.KindSink, Name: "y"},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected panic", name)
				}
			}()
			registry.Register(i)
		}()
	}
}

func TestConcurrent(t *testing.T) {
	registry.Reset()
	t.Cleanup(registry.Reset)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registry.Register(info(contract.KindSink, string(rune('a'+i%26))+string(rune('a'+i/26))))
			registry.List("")
		}()
	}
	wg.Wait()
	if n := len(registry.List("")); n != 50 {
		t.Fatalf("got %d modules, want 50", n)
	}
}
