package semantic

import (
	"sort"
	"strings"
)

// FieldInfo describes a field for clients.
type FieldInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Label       string `json:"label,omitempty"`
	Datatype    string `json:"datatype,omitempty"`
	IsTime      bool   `json:"is_time,omitempty"`
	AIContext   any    `json:"ai_context,omitempty"`
}

// DatasetInfo describes a dataset the role can read.
type DatasetInfo struct {
	Name        string      `json:"name"`
	Source      string      `json:"source"`
	Description string      `json:"description,omitempty"`
	AIContext   any         `json:"ai_context,omitempty"`
	Fields      []FieldInfo `json:"fields"`
}

// RelationshipInfo describes a many-to-one join between two readable datasets.
type RelationshipInfo struct {
	Name        string   `json:"name"`
	From        string   `json:"from"`
	To          string   `json:"to"`
	FromColumns []string `json:"from_columns"`
	ToColumns   []string `json:"to_columns"`
	AIContext   any      `json:"ai_context,omitempty"`
}

// MetricInfo describes a metric the role can compute.
type MetricInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Datatype    string `json:"datatype,omitempty"`
	Dataset     string `json:"dataset"`
	AIContext   any    `json:"ai_context,omitempty"`
	// Dimensions are the "dataset.field" names it can be grouped or filtered
	// by (reachable without duplicating rows).
	Dimensions []string `json:"dimensions"`
}

// ModelInfo is what describe returns.
type ModelInfo struct {
	Models        []DocInfo          `json:"models"`
	Datasets      []DatasetInfo      `json:"datasets"`
	Relationships []RelationshipInfo `json:"relationships"`
	Metrics       []MetricInfo       `json:"metrics"`
}

// DocInfo is the header of a model document.
type DocInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	AIContext   any    `json:"ai_context,omitempty"`
}

// Describe lists what the scope can use: datasets the role has a view of, the
// relationships between them, and the metrics computable on them. Hidden
// objects are left out.
func (m *Model) Describe(sc Scope) ModelInfo {
	info := ModelInfo{Models: []DocInfo{}, Datasets: []DatasetInfo{}, Relationships: []RelationshipInfo{}, Metrics: m.ListMetrics(sc)}
	for _, d := range m.Docs {
		info.Models = append(info.Models, DocInfo{Name: d.Name, Description: d.Description, AIContext: d.AIContext})
	}
	for _, d := range m.Datasets {
		if d.Hidden || !sc.has(d) {
			continue
		}
		di := DatasetInfo{Name: d.Name, Source: d.Source, Description: d.Description, AIContext: d.AIContext, Fields: []FieldInfo{}}
		for _, f := range d.Fields {
			if f.Hidden {
				continue
			}
			di.Fields = append(di.Fields, FieldInfo{Name: f.Name, Description: f.Description, Label: f.Label,
				Datatype: f.Datatype, IsTime: f.IsTime(), AIContext: f.AIContext})
		}
		info.Datasets = append(info.Datasets, di)
	}
	for _, r := range m.Relationships {
		if r.from.Hidden || r.to.Hidden || !sc.has(r.from) || !sc.has(r.to) {
			continue
		}
		info.Relationships = append(info.Relationships, RelationshipInfo{Name: r.Name, From: r.From, To: r.To,
			FromColumns: r.FromColumns, ToColumns: r.ToColumns, AIContext: r.AIContext})
	}
	return info
}

// ListMetrics lists the metrics computable in the scope, with their dimensions.
func (m *Model) ListMetrics(sc Scope) []MetricInfo {
	out := []MetricInfo{}
	for _, mt := range m.Metrics {
		if mt.Hidden || mt.home.Hidden || !sc.has(mt.home) {
			continue
		}
		out = append(out, MetricInfo{Name: mt.Name, Description: mt.Description, Datatype: mt.Datatype,
			Dataset: mt.home.Name, AIContext: mt.AIContext, Dimensions: m.dimensionsOf(sc, mt.home)})
	}
	return out
}

// dimensionsOf lists the visible fields of fact and of every dataset reachable
// from it by many-to-one relationships that the scope can read.
func (m *Model) dimensionsOf(sc Scope, fact *Dataset) []string {
	seen := map[*Dataset]bool{fact: true}
	order := []*Dataset{fact}
	for i := 0; i < len(order); i++ {
		for _, r := range m.Relationships {
			if r.from == order[i] && !seen[r.to] && sc.has(r.to) {
				seen[r.to] = true
				order = append(order, r.to)
			}
		}
	}
	var out []string
	for _, d := range order {
		if d.Hidden {
			continue
		}
		for _, f := range d.Fields {
			if !f.Hidden {
				out = append(out, d.Name+"."+f.Name)
			}
		}
	}
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	return out
}

// TableSet builds the lookup set of Scope.Tables.
func TableSet(names ...string) map[string]bool {
	m := map[string]bool{}
	for _, n := range names {
		m[strings.ToLower(n)] = true
	}
	return m
}
