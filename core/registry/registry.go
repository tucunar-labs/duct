// Package registry holds the modules compiled into the binary. Modules call
// Register from init(); the core looks them up by kind and name.
package registry

import (
	"fmt"
	"sort"
	"sync"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Info describes a registrable module.
type Info struct {
	Kind contract.Kind
	Name string
	// New returns a fresh, uninitialised instance. It must not do I/O.
	New func() contract.Module
}

type key struct {
	kind contract.Kind
	name string
}

var (
	mu      sync.RWMutex
	modules = map[key]Info{}
)

// Register adds a module. It panics on an empty kind or name, a nil New, or a
// duplicate (kind, name): these are programming errors caught at start-up.
func Register(info Info) {
	if info.Kind == "" || info.Name == "" || info.New == nil {
		panic("registry: Register requires Kind, Name and New")
	}
	k := key{info.Kind, info.Name}
	mu.Lock()
	defer mu.Unlock()
	if _, dup := modules[k]; dup {
		panic(fmt.Sprintf("registry: duplicate module %s/%s", info.Kind, info.Name))
	}
	modules[k] = info
}

// Lookup returns the module registered under kind and name.
func Lookup(kind contract.Kind, name string) (Info, bool) {
	mu.RLock()
	defer mu.RUnlock()
	info, ok := modules[key{kind, name}]
	return info, ok
}

// List returns all registered modules sorted by kind then name. If kind is
// non-empty only that kind is returned.
func List(kind contract.Kind) []Info {
	mu.RLock()
	out := make([]Info, 0, len(modules))
	for k, info := range modules {
		if kind == "" || k.kind == kind {
			out = append(out, info)
		}
	}
	mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Reset empties the registry. Intended for tests only.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	modules = map[key]Info{}
}
