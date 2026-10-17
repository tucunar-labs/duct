package main

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pipeline"
	"github.com/alonsoburon/duct/core/project"
)

// openRunEngine opens an initialised project's engine in-process, so staging
// state (which lives in the engine) can be inspected after runs.
func openRunEngine(t *testing.T) contract.Engine {
	t.Helper()
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	p, err := findProject(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	e, err := openEngine(ctx, p, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := project.Migrate(ctx, e); err != nil {
		t.Fatal(err)
	}
	return e
}

func scalar(t *testing.T, e contract.Engine, q string) int64 {
	t.Helper()
	st, err := e.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer st.Release()
	if !st.Next() {
		t.Fatalf("%s: no rows", q)
	}
	n, err := strconv.ParseInt(st.RecordBatch().Column(0).ValueStr(0), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func parseTables(t *testing.T, src string) []pipeline.Table {
	t.Helper()
	tables, err := pipeline.Parse("t.sql", src)
	if err != nil {
		t.Fatal(err)
	}
	return tables
}

func noStage(t *testing.T, e contract.Engine) {
	t.Helper()
	if n := scalar(t, e, "SELECT count(*) FROM duckdb_tables() WHERE database_name = '"+pipeline.StageCatalog+"'"); n != 0 {
		t.Errorf("%d staging tables left behind", n)
	}
	if n := scalar(t, e, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name LIKE '%duct_exp%'"); n != 0 {
		t.Errorf("staging leaked into the lake")
	}
}

// A non-deterministic SELECT and expectation must be evaluated once: the
// violations counted are exactly the rows dropped, and written + dropped is
// the number of rows staged. With two evaluations these disagree.
func TestRefreshEvaluatesOnce(t *testing.T) {
	t.Parallel()
	e := openRunEngine(t)
	ctx := context.Background()
	const n = 20000
	tables := parseTables(t, `CREATE MATERIALIZED VIEW s.r (
  CONSTRAINT coin EXPECT (r < 0.5) ON VIOLATION DROP ROW,
  CONSTRAINT coin_warn EXPECT (r < 0.5)
) AS SELECT i, random() AS r FROM range(`+strconv.Itoa(n)+`) t(i);`)
	for range 3 {
		rep, err := pipeline.Run(ctx, e, tables, pipeline.Options{})
		if err != nil || rep.Failed() {
			t.Fatalf("run: %v %+v", err, rep)
		}
		res := rep.Results[0]
		written := scalar(t, e, "SELECT count(*) FROM s.r")
		if written != res.Rows || written+res.Dropped != n {
			t.Errorf("written=%d reported=%d dropped=%d staged=%d", written, res.Rows, res.Dropped, n)
		}
		for _, v := range res.Violations {
			if v.Count != res.Dropped {
				t.Errorf("%s counted %d, dropped %d", v.Name, v.Count, res.Dropped)
			}
		}
		if bad := scalar(t, e, "SELECT count(*) FROM s.r WHERE r >= 0.5"); bad != 0 {
			t.Errorf("%d violating rows were written", bad)
		}
		if scalar(t, e, "SELECT count(*) FROM information_schema.columns WHERE table_name = 's' OR column_name LIKE '__duct%'") != 0 {
			t.Error("helper columns leaked into the target")
		}
		noStage(t, e)
	}
}

// FAIL UPDATE on a non-deterministic source aborts the table, keeps the
// previous version and leaves no staging behind. NULL counts as violation.
func TestRefreshFailUpdateKeepsPrevious(t *testing.T) {
	t.Parallel()
	e := openRunEngine(t)
	ctx := context.Background()
	ok := parseTables(t, `CREATE MATERIALIZED VIEW s.t AS SELECT 1 AS x, 'keep' AS tag;`)
	if rep, err := pipeline.Run(ctx, e, ok, pipeline.Options{}); err != nil || rep.Failed() {
		t.Fatalf("seed: %v", err)
	}
	bad := parseTables(t, `CREATE MATERIALIZED VIEW s.t (
  CONSTRAINT not_null EXPECT (x) ON VIOLATION FAIL UPDATE
) AS SELECT NULL::BOOLEAN AS x, 'new' AS tag;
CREATE MATERIALIZED VIEW s.down AS SELECT * FROM s.t;`)
	rep, err := pipeline.Run(ctx, e, bad, pipeline.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Results[0].Status != pipeline.StatusFailed || rep.Results[0].Violations[0].Count != 1 ||
		rep.Results[1].Status != pipeline.StatusSkipped {
		t.Fatalf("results = %+v", rep.Results)
	}
	if scalar(t, e, "SELECT count(*) FROM s.t WHERE tag = 'keep'") != 1 || scalar(t, e, "SELECT count(*) FROM s.t") != 1 {
		t.Error("previous version not intact")
	}
	if scalar(t, e, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'down'") != 0 {
		t.Error("downstream was refreshed")
	}
	noStage(t, e)
}
