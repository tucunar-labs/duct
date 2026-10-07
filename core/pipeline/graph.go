package pipeline

import (
	"fmt"
	"sort"
	"strings"
)

// Graph is the dependency graph among pipeline outputs. Dependencies on
// tables that are not pipeline outputs (Bronze sources) are not nodes.
type Graph struct {
	deps  map[string][]string // node -> upstream nodes (sorted)
	order []string            // global topological order, upstream first
	known map[string]bool
}

// NewGraph builds the graph from node -> referenced ids. References to ids
// that are not keys of nodes are treated as external sources and dropped.
// It fails with the cycle path (a -> b -> a) if there is a cycle.
func NewGraph(nodes map[string][]string) (*Graph, error) {
	g := &Graph{deps: map[string][]string{}, known: map[string]bool{}}
	for id := range nodes {
		g.known[id] = true
	}
	for id, refs := range nodes {
		seen := map[string]bool{}
		for _, r := range refs {
			if _, ok := nodes[r]; ok && !seen[r] {
				seen[r] = true
				g.deps[id] = append(g.deps[id], r)
			}
		}
		sort.Strings(g.deps[id])
	}
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	const (
		visiting = 1
		done     = 2
	)
	state := map[string]int{}
	var stack []string
	var visit func(id string) error
	visit = func(id string) error {
		switch state[id] {
		case done:
			return nil
		case visiting:
			i := len(stack) - 1
			for stack[i] != id {
				i--
			}
			cyc := append(append([]string{}, stack[i:]...), id)
			return fmt.Errorf("dependency cycle: %s", strings.Join(cyc, " -> "))
		}
		state[id] = visiting
		stack = append(stack, id)
		for _, d := range g.deps[id] {
			if err := visit(d); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = done
		g.order = append(g.order, id)
		return nil
	}
	for _, id := range ids {
		if err := visit(id); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// Upstream returns the direct upstream nodes of id.
func (g *Graph) Upstream(id string) []string { return g.deps[id] }

// Plan returns the nodes to run, upstream first. With no targets it is every
// node. With targets it is the targets plus, if withUpstream, everything they
// depend on. Unknown targets are an error.
func (g *Graph) Plan(targets []string, withUpstream bool) ([]string, error) {
	if len(targets) == 0 {
		return append([]string{}, g.order...), nil
	}
	want := map[string]bool{}
	var add func(id string)
	add = func(id string) {
		if want[id] {
			return
		}
		want[id] = true
		if withUpstream {
			for _, d := range g.deps[id] {
				add(d)
			}
		}
	}
	for _, t := range targets {
		if !g.known[t] {
			return nil, fmt.Errorf("unknown pipeline target %q", t)
		}
		add(t)
	}
	var out []string
	for _, id := range g.order {
		if want[id] {
			out = append(out, id)
		}
	}
	return out, nil
}
