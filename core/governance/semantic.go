package governance

import (
	"context"
	"fmt"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/semantic"
)

type semanticModel = semantic.Model

// SetSemanticModel gives the handler the semantic model (ADR 0011) so
// protocols can ask for metrics through contract.SemanticQuerier. nil removes it.
func (h *Handler) SetSemanticModel(m *semantic.Model) { h.semModel.Store(m) }

var _ contract.SemanticQuerier = (*Handler)(nil)

// scope lists the views the identity's role owns.
func (h *Handler) scope(ctx context.Context, id contract.Identity) (semantic.Scope, error) {
	st, err := h.Catalog(ctx, id)
	if err != nil {
		return semantic.Scope{}, err
	}
	defer st.Release()
	sc := semantic.Scope{Role: id.Role, Tables: map[string]bool{}}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			sc.Tables[strings.ToLower(strings.Clone(rec.Column(1).ValueStr(i)))] = true
		}
	}
	return sc, st.Err()
}

// SemanticQuery implements contract.SemanticQuerier (audit protocol "unknown").
func (h *Handler) SemanticQuery(ctx context.Context, id contract.Identity, req contract.SemanticRequest) (*contract.SemanticResult, error) {
	return h.semanticQuery(ctx, "unknown", id, req)
}

func (h *Handler) semanticQuery(ctx context.Context, proto string, id contract.Identity, req contract.SemanticRequest) (*contract.SemanticResult, error) {
	m := h.semModel.Load()
	if m == nil {
		return nil, fmt.Errorf("%w: create semantic/*.ossie.yaml", contract.ErrNoSemanticModel)
	}
	sc, err := h.scope(ctx, id)
	if err != nil {
		return nil, err
	}
	sr := semantic.Request{Dims: req.Dims, Measures: req.Measures, Limit: req.Limit}
	for _, f := range req.Filters {
		sr.Filters = append(sr.Filters, semantic.Filter{Field: f.Field, Metric: f.Metric, Op: f.Op, Value: f.Value})
	}
	for _, o := range req.Order {
		sr.Order = append(sr.Order, semantic.Order{By: o.By, Desc: o.Desc})
	}
	sc.Fresh = h.freshPreaggs(ctx, m, sc)
	c, err := m.Query(sc, sr)
	if err != nil {
		return nil, err
	}
	// From here the statement is audited like any other. A query routed to a
	// preaggregate reads semantic_cache, which only compiler output may do.
	qctx := ctx
	if c.Preagg != "" {
		qctx = withCompiled(ctx)
	}
	st, err := h.query(qctx, proto, id, c.SQL)
	if err != nil {
		return nil, err
	}
	out := &contract.SemanticResult{Stream: st, SQL: c.SQL}
	for _, col := range c.Columns {
		out.Columns = append(out.Columns, contract.SemanticColumn{Name: col.Name, Kind: col.Kind, Datatype: col.Datatype})
	}
	return out, nil
}

// LakeSnapshot implements contract.SemanticQuerier: the latest DuckLake
// snapshot id.
func (h *Handler) LakeSnapshot(ctx context.Context) (string, error) {
	return h.scalar(ctx, "SELECT COALESCE(max(snapshot_id), 0) FROM lake.snapshots()")
}

// SemanticQuery forwards to the Handler, labelling the audit record.
func (p *protocolHandler) SemanticQuery(ctx context.Context, id contract.Identity, req contract.SemanticRequest) (*contract.SemanticResult, error) {
	return p.h.semanticQuery(ctx, p.name, id, req)
}

// LakeSnapshot forwards to the Handler.
func (p *protocolHandler) LakeSnapshot(ctx context.Context) (string, error) {
	return p.h.LakeSnapshot(ctx)
}
