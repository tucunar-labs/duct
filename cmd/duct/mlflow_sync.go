package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/txretry"
)

// MLflow -> Bronze (ADR 0010). The MLflow backend database (attached read only as
// catalog "mlflow") is copied into bronze.mlflow_{experiments,runs,params,metrics,tags}
// so runs can be governed, joined and dashboarded like any other table.
//
// Idempotent and incremental. Every table is written by key from rows that differ from
// what Bronze already has, so repeating a pass changes nothing (and, with nothing new,
// commits no snapshot); the time watermarks (duct_ingest_state, names mlflow:*) only
// narrow the rows each pass has to look at:
//
//	experiments, runs   upsert by key; window = last_update_time / greatest(start, end, deleted) >= mark
//	params              insert the missing ones of the runs in the window (MLflow params are immutable)
//	tags                replace the tags of the window runs whose set changed (count + hash)
//	metrics             insert the missing points (run, key, step, timestamp, value) with timestamp >= mark - lookback
//
// Limits: a metric logged with a timestamp older than mark - lookback, and tags set on a
// run that is no longer in the window, are picked up by `duct mlflow sync --full`.
// Rows deleted by `mlflow gc` stay in Bronze (history), runs "deleted" in MLflow carry
// lifecycle_stage = 'deleted'. A run whose timestamp is NULL is only copied by a full pass.
const (
	mlflowSyncLease = "mlflow-sync"
	metricsLookback = 10 * time.Minute
)

// mlflowTable describes one Bronze table: its SELECT over the attached catalog.
var mlflowSelect = map[string]string{
	"experiments": `SELECT experiment_id, name, artifact_location, lifecycle_stage, creation_time, last_update_time,
		epoch_ms(creation_time) AS creation_ts, epoch_ms(last_update_time) AS last_update_ts FROM mlflow.experiments`,
	"runs": `SELECT run_uuid, name, source_type, source_name, entry_point_name, user_id, status, start_time, end_time,
		source_version, lifecycle_stage, artifact_uri, experiment_id, deleted_time,
		epoch_ms(start_time) AS start_ts, epoch_ms(end_time) AS end_ts FROM mlflow.runs`,
	"params": `SELECT run_uuid, "key", value FROM mlflow.params`,
	"tags":   `SELECT run_uuid, "key", value FROM mlflow.tags`,
	"metrics": `SELECT run_uuid, "key", CAST(value AS DOUBLE) AS value, timestamp, step, is_nan,
		epoch_ms(timestamp) AS ts FROM mlflow.metrics`,
}

const (
	runTS = "greatest(coalesce(start_time, 0), coalesce(end_time, 0), coalesce(deleted_time, 0))"
	expTS = "greatest(coalesce(creation_time, 0), coalesce(last_update_time, 0))"
)

// MLflowSyncResult counts the rows written to each Bronze table.
type MLflowSyncResult struct {
	Experiments, Runs, Params, Tags, Metrics int64
}

func (r MLflowSyncResult) String() string {
	return fmt.Sprintf("experiments=%d runs=%d params=%d tags=%d metrics=%d", r.Experiments, r.Runs, r.Params, r.Tags, r.Metrics)
}

func (r MLflowSyncResult) total() int64 {
	return r.Experiments + r.Runs + r.Params + r.Tags + r.Metrics
}

// syncMLflow copies what changed since the last pass; full ignores the watermarks.
// eng must be unlocked, with the mlflow catalog attached and the Duct tables migrated.
func syncMLflow(ctx context.Context, eng contract.Engine, full bool) (res MLflowSyncResult, err error) {
	tr, ok := eng.(contract.Transactor)
	if !ok {
		return res, errors.New("engine does not implement contract.Transactor")
	}
	if rows, qerr := queryAll(ctx, eng, "SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'mlflow' AND table_name = 'runs'"); qerr != nil || len(rows) == 0 || rows[0][0] == "0" {
		return res, errors.New("the mlflow database is not attached or empty (has `duct mlflow` run once?)")
	}
	lk, err := ingest.Acquire(ctx, eng, mlflowSyncLease)
	if err != nil {
		return res, err
	}
	defer func() {
		if rerr := lk.Release(ctx); rerr != nil && err == nil {
			err = rerr
		}
	}()
	if err := txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return eng.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+ingest.BronzeSchema)
	}); err != nil {
		return res, err
	}

	mark := func(name string) (int64, error) {
		if full {
			return 0, nil
		}
		rows, err := queryAll(ctx, eng, "SELECT watermark FROM duct.duct_ingest_state WHERE name = 'mlflow:"+name+"'")
		if err != nil || len(rows) == 0 || rows[0][0] == "" {
			return 0, err
		}
		return strconv.ParseInt(rows[0][0], 10, 64)
	}
	runMark, err := mark("runs")
	if err != nil {
		return res, err
	}
	expMark, err := mark("experiments")
	if err != nil {
		return res, err
	}
	metMark, err := mark("metrics")
	if err != nil {
		return res, err
	}
	// New marks are read before copying: what arrives meanwhile is still >= the mark next pass.
	maxOf := func(sql string) (int64, error) {
		rows, err := queryAll(ctx, eng, sql)
		if err != nil || len(rows) == 0 || rows[0][0] == "" {
			return 0, err
		}
		return strconv.ParseInt(rows[0][0], 10, 64)
	}
	newRun, err := maxOf("SELECT max(" + runTS + ") FROM mlflow.runs")
	if err != nil {
		return res, err
	}
	newExp, err := maxOf("SELECT max(" + expTS + ") FROM mlflow.experiments")
	if err != nil {
		return res, err
	}
	newMet, err := maxOf("SELECT max(timestamp) FROM mlflow.metrics")
	if err != nil {
		return res, err
	}
	metFrom := max(metMark-metricsLookback.Milliseconds(), 0)

	create := func(tx contract.Tx, name string) error {
		return tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS bronze.mlflow_"+name+" AS "+mlflowSelect[name]+" WHERE false")
	}
	runsWindow := fmt.Sprintf("SELECT run_uuid FROM mlflow.runs WHERE %s >= %d", runTS, runMark)
	err = txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		res = MLflowSyncResult{}
		return tr.InTx(ctx, nil, func(ctx context.Context, tx contract.Tx) error {
			for _, n := range []string{"experiments", "runs", "params", "tags", "metrics"} {
				if err := create(tx, n); err != nil {
					return err
				}
			}
			n := func(stmt string, into *int64) error {
				v, err := txCount(ctx, tx, stmt)
				*into += v
				return err
			}
			// experiments and runs: the rows that are new or differ, upserted by key.
			if err := tx.Exec(ctx, `CREATE OR REPLACE TEMP TABLE _mlflow_exp AS `+mlflowSelect["experiments"]+
				fmt.Sprintf(` s WHERE %s >= %d AND NOT EXISTS (SELECT 1 FROM bronze.mlflow_experiments b WHERE b.experiment_id = s.experiment_id
				AND b.name IS NOT DISTINCT FROM s.name AND b.lifecycle_stage IS NOT DISTINCT FROM s.lifecycle_stage
				AND b.last_update_time IS NOT DISTINCT FROM s.last_update_time)`, expTS, expMark)); err != nil {
				return err
			}
			if err := tx.Exec(ctx, "DELETE FROM bronze.mlflow_experiments WHERE experiment_id IN (SELECT experiment_id FROM _mlflow_exp)"); err != nil {
				return err
			}
			if err := n("INSERT INTO bronze.mlflow_experiments SELECT * FROM _mlflow_exp", &res.Experiments); err != nil {
				return err
			}
			if err := tx.Exec(ctx, `CREATE OR REPLACE TEMP TABLE _mlflow_run AS `+mlflowSelect["runs"]+
				fmt.Sprintf(` s WHERE %s >= %d AND NOT EXISTS (SELECT 1 FROM bronze.mlflow_runs b WHERE b.run_uuid = s.run_uuid
				AND b.name IS NOT DISTINCT FROM s.name AND b.status IS NOT DISTINCT FROM s.status
				AND b.end_time IS NOT DISTINCT FROM s.end_time AND b.lifecycle_stage IS NOT DISTINCT FROM s.lifecycle_stage
				AND b.deleted_time IS NOT DISTINCT FROM s.deleted_time)`, runTS, runMark)); err != nil {
				return err
			}
			if err := tx.Exec(ctx, "DELETE FROM bronze.mlflow_runs WHERE run_uuid IN (SELECT run_uuid FROM _mlflow_run)"); err != nil {
				return err
			}
			if err := n("INSERT INTO bronze.mlflow_runs SELECT * FROM _mlflow_run", &res.Runs); err != nil {
				return err
			}
			// params: immutable, so only the missing ones of the window runs.
			if err := n(`INSERT INTO bronze.mlflow_params SELECT * FROM (`+mlflowSelect["params"]+`) s WHERE run_uuid IN (`+runsWindow+`)
				AND NOT EXISTS (SELECT 1 FROM bronze.mlflow_params b WHERE b.run_uuid = s.run_uuid AND b."key" = s."key")`, &res.Params); err != nil {
				return err
			}
			// tags: mutable; replace the tag set of the window runs whose (count, hash) differ.
			sig := func(table, where string) string {
				return `SELECT run_uuid, count(*) AS n, sum(hash("key" || chr(0) || coalesce(value, ''))::HUGEINT) AS h FROM ` + table +
					` WHERE run_uuid IN (` + where + `) GROUP BY run_uuid`
			}
			if err := tx.Exec(ctx, `CREATE OR REPLACE TEMP TABLE _mlflow_tagruns AS
				SELECT coalesce(s.run_uuid, b.run_uuid) AS run_uuid FROM (`+sig("mlflow.tags", runsWindow)+`) s
				FULL JOIN (`+sig("bronze.mlflow_tags", runsWindow)+`) b ON s.run_uuid = b.run_uuid
				WHERE s.n IS DISTINCT FROM b.n OR s.h IS DISTINCT FROM b.h`); err != nil {
				return err
			}
			if err := tx.Exec(ctx, "DELETE FROM bronze.mlflow_tags WHERE run_uuid IN (SELECT run_uuid FROM _mlflow_tagruns)"); err != nil {
				return err
			}
			if err := n(`INSERT INTO bronze.mlflow_tags `+mlflowSelect["tags"]+` WHERE run_uuid IN (SELECT run_uuid FROM _mlflow_tagruns)`, &res.Tags); err != nil {
				return err
			}
			// metrics: append only.
			return n(fmt.Sprintf(`INSERT INTO bronze.mlflow_metrics SELECT * FROM (`+mlflowSelect["metrics"]+`) s WHERE timestamp >= %d
				AND NOT EXISTS (SELECT 1 FROM bronze.mlflow_metrics b WHERE b.run_uuid = s.run_uuid AND b."key" = s."key"
				AND b.step = s.step AND b.timestamp = s.timestamp AND b.value = s.value)`, metFrom), &res.Metrics)
		})
	})
	if err != nil {
		return res, fmt.Errorf("copy to bronze: %w", err)
	}
	for name, v := range map[string]int64{"runs": newRun, "experiments": newExp, "metrics": newMet} {
		if v == 0 {
			continue
		}
		q := "'mlflow:" + name + "'"
		if err := eng.ExecTx(ctx,
			"DELETE FROM duct.duct_ingest_state WHERE name = "+q,
			fmt.Sprintf("INSERT INTO duct.duct_ingest_state (name, watermark, rows_loaded, updated_at) VALUES (%s, '%d', %d, now())", q, v, res.total())); err != nil {
			return res, fmt.Errorf("data copied but marks not saved (the next pass repeats nothing, only rescans): %w", err)
		}
	}
	return res, nil
}

// txCount runs an INSERT/CTAS inside tx and returns its affected-row count.
func txCount(ctx context.Context, tx contract.Tx, stmt string) (int64, error) {
	st, err := tx.Query(ctx, stmt)
	if err != nil {
		return 0, err
	}
	defer st.Release()
	var n int64
	for st.Next() {
		if rec := st.RecordBatch(); rec.NumRows() > 0 && rec.Column(0).IsValid(0) {
			n, _ = strconv.ParseInt(rec.Column(0).ValueStr(0), 10, 64)
		}
	}
	return n, st.Err()
}

// cmdMLflowSync is `duct mlflow sync [--full]`.
func cmdMLflowSync(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("mlflow sync", stderr)
	full := fs.Bool("full", false, "ignore the watermarks and rescan everything (idempotent)")
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if p.Config.MLflow == nil {
		return errors.New("mlflow is not enabled: run `duct enable mlflow`")
	}
	eng, err := openEngineUDF(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		return err
	}
	start := time.Now()
	res, err := syncMLflow(ctx, eng, *full)
	ca := newCLIAudit(p, eng)
	defer ca.close()
	ca.record("duct mlflow sync", start, res.total(), err)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "mlflow -> bronze.mlflow_*: %s (%s)\n", res, time.Since(start).Round(time.Millisecond))
	return nil
}

// startMLflowSync runs `[mlflow] sync = true` inside `duct serve`: one pass at start and one
// every sync_interval (default 5m), on its own engine. A failing pass is logged, not fatal
// (MLflow may not have created its database yet).
func startMLflowSync(ctx context.Context, p *project.Project) (wait func()) {
	if p.Config.MLflow == nil || !p.Config.MLflow.Sync {
		return func() {}
	}
	every, err := p.MLflowSyncInterval()
	if err != nil {
		slog.Error("mlflow sync disabled", "error", err)
		return func() {}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pass := func() {
			eng, err := openEngineUDF(ctx, p, false)
			if err != nil {
				slog.Warn("mlflow sync", "error", err)
				return
			}
			defer func() { _ = eng.Close() }()
			if _, err := project.Migrate(ctx, eng); err != nil {
				slog.Warn("mlflow sync", "error", err)
				return
			}
			res, err := syncMLflow(ctx, eng, false)
			if err != nil && ctx.Err() == nil {
				slog.Warn("mlflow sync", "error", err)
			} else if res.total() > 0 {
				slog.Info("mlflow sync", "copied", res.String())
			}
		}
		t := time.NewTicker(every)
		defer t.Stop()
		pass()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pass()
			}
		}
	}()
	return wg.Wait
}
