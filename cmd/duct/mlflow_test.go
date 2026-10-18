package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestEnableMLflow(t *testing.T) {
	t.Parallel()
	root := newProject(t)
	if _, errs, code := duct(t, "enable", "mlflow", "--listen", "0.0.0.0:5000", "--project", root); code != 2 || !strings.Contains(errs, "loopback") {
		t.Fatalf("public listen must be refused: code=%d err=%q", code, errs)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "duct.toml")); strings.Contains(string(b), "[mlflow]") {
		t.Fatal("a refused enable wrote the config")
	}
	out := mustDuct(t, "enable", "mlflow", "--project", root)
	if !strings.Contains(out, ".duct/mlflow.sqlite") || !strings.Contains(out, "lake/_mlflow") {
		t.Fatalf("enable output: %q", out)
	}
	b, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	if !strings.Contains(string(b), "[mlflow]") || !strings.Contains(string(b), "mlflow==") {
		t.Fatalf("duct.toml: %s", b)
	}
	if out := mustDuct(t, "enable", "mlflow", "--project", root); !strings.Contains(out, "already enabled") {
		t.Fatalf("second enable: %q", out)
	}
	// The project still loads, and `duct query` works with no MLflow database yet.
	if got := queryCSV(t, root, "SELECT 1 AS x"); got != "x\n1" {
		t.Fatalf("query: %q", got)
	}
	// What would run (no secrets in the output).
	pr := mustDuct(t, "mlflow", "--print", "--project", root)
	for _, want := range []string{"-m mlflow server", "--host 127.0.0.1", "--artifacts-destination file://", "/lake/_mlflow", "sqlite:///", ".duct/mlflow.sqlite"} {
		if !strings.Contains(pr, want) {
			t.Errorf("mlflow --print lacks %q:\n%s", want, pr)
		}
	}
	// Not enabled: a clear error.
	other := newProject(t)
	if _, errs, code := duct(t, "mlflow", "--project", other); code != 1 || !strings.Contains(errs, "duct enable mlflow") {
		t.Fatalf("not enabled: code=%d err=%q", code, errs)
	}
}

// TestMLflowIntegration installs MLflow (pinned) into the managed venv, runs `duct mlflow`, logs a
// model and scores rows with it from SQL. Heavy: skipped with -short, without uv or offline.
func TestMLflowIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in -short mode: installs MLflow")
	}
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv is not installed")
	}
	_, file, _, _ := runtime.Caller(0)
	sdk := filepath.Join(filepath.Dir(file), "..", "..", "python", "duct-data")
	t.Setenv("DUCT_PYTHON", "")
	t.Setenv("DUCT_SDK", sdk+"[arrow,pg]")
	t.Setenv("MLFLOW_TRACKING_URI", "")

	root := newProject(t)
	addr := freePort(t)
	mustDuct(t, "enable", "mlflow", "--listen", addr, "--project", root)

	ctx, cancel := context.WithCancel(context.Background())
	var out, errb bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- cmdMLflow(ctx, []string{"--project", root}, &out, &errb) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Error("duct mlflow did not stop")
		}
	}
	defer stop()

	deadline := time.Now().Add(15 * time.Minute)
	for ready := false; !ready; {
		select {
		case err := <-done:
			stopped = true
			if strings.Contains(err.Error(), "dns error") || strings.Contains(err.Error(), "Could not resolve") {
				t.Skipf("offline, cannot install MLflow: %v", err)
			}
			t.Fatalf("duct mlflow exited: %v\n%s", err, errb.String())
		default:
		}
		if resp, err := http.Get("http://" + addr + "/health"); err == nil {
			_ = resp.Body.Close()
			ready = resp.StatusCode == 200
		}
		if !ready {
			if time.Now().After(deadline) {
				t.Fatalf("MLflow did not come up:\n%s", errb.String())
			}
			time.Sleep(time.Second)
		}
	}

	py := filepath.Join(root, ".duct", "venv", "bin", "python")
	cmd := exec.Command(py, "-c", `
import mlflow, mlflow.pyfunc, pandas as pd
mlflow.set_experiment("demo")
class M(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input, params=None):
        return model_input["tenure"] * 2 + model_input["spend"]
with mlflow.start_run():
    mlflow.log_param("alpha", 0.5)
    mlflow.log_metric("auc", 0.91)
    mlflow.pyfunc.log_model(name="model", python_model=M(), registered_model_name="churn",
        pip_requirements=["mlflow"], input_example=pd.DataFrame({"tenure": [1.0], "spend": [2.0]}))
`)
	cmd.Env = append(os.Environ(), "MLFLOW_TRACKING_URI=http://"+addr)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("log model: %v\n%s", err, b)
	}
	// Metadata is queryable from SQL, read only.
	if got := queryCSV(t, root, "SELECT status FROM mlflow.runs"); got != "status\nFINISHED" {
		t.Fatalf("mlflow.runs: %q", got)
	}
	if got := queryCSV(t, root, "SELECT key, value FROM mlflow.metrics"); got != "key,value\nauc,0.91" {
		t.Fatalf("mlflow.metrics: %q", got)
	}
	if _, errs, code := duct(t, "query", "--project", root, "DELETE FROM mlflow.runs"); code == 0 {
		t.Fatalf("mlflow must be read only (err=%q)", errs)
	}
	// Roles of duct serve never see it: ATTACH is closed in locked mode and they only get their views.
	if _, _, code := duct(t, "query", "--locked", "--project", root, "SELECT * FROM mlflow.runs"); code == 0 {
		t.Error("locked engine must not read the MLflow database")
	}
	// Artifacts landed in the lake folder, and gc leaves them alone.
	count := func() (n int) {
		_ = filepath.WalkDir(filepath.Join(root, "lake", "_mlflow"), func(_ string, d os.DirEntry, _ error) error {
			if d != nil && !d.IsDir() {
				n++
			}
			return nil
		})
		return n
	}
	before := count()
	if before == 0 {
		t.Fatal("no artifacts under lake/_mlflow")
	}
	mustDuct(t, "gc", "--project", root, "--keep-snapshots", "1s")
	if after := count(); after != before {
		t.Fatalf("gc touched lake/_mlflow: %d -> %d files", before, after)
	}

	// Batch scoring from SQL: duct.mlflow.score + the UDF worker.
	writeFile(t, filepath.Join(root, "functions", "score.py"), `import duct.mlflow

duct.mlflow.score(
    "models:/churn/1", name="churn_score", inputs=["DOUBLE", "DOUBLE"],
    columns=["tenure", "spend"], returns="DOUBLE",
)
`)
	t.Setenv("DUCT_PYTHON", py)
	t.Setenv("MLFLOW_TRACKING_URI", "http://"+addr)
	got := queryCSV(t, root, "SELECT i, churn_score(i::DOUBLE, 10.0) AS s FROM range(3) t(i) ORDER BY i")
	if got != "i,s\n0,10\n1,12\n2,14" {
		t.Fatalf("score: %q", got)
	}

	mlflowSyncStage(t, root, py, addr)
	mlflowServeStage(t, root, py, addr)
	stop()
}

// runPy runs a Python snippet with the tracking URI of `duct mlflow`.
func runPy(t *testing.T, root, py, addr, code string) {
	t.Helper()
	cmd := exec.Command(py, "-c", code)
	cmd.Dir = filepath.Join(root, ".duct")
	cmd.Env = append(os.Environ(), "MLFLOW_TRACKING_URI=http://"+addr)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("python: %v\n%s", err, b)
	}
}

// mlflowSyncStage: `duct mlflow sync` copies experiments, runs, params, tags and metrics to
// Bronze; a second pass copies nothing; later runs and a run that finishes are picked up
// without duplicates; --full is idempotent too.
func mlflowSyncStage(t *testing.T, root, py, addr string) {
	t.Helper()
	// Cold start: demo has one run (auc, alpha); the model registry also logged its own rows.
	out := mustDuct(t, "mlflow", "sync", "--project", root)
	if !strings.Contains(out, "runs=1") || !strings.Contains(out, "metrics=1") || !strings.Contains(out, "params=1") {
		t.Fatalf("first sync: %q", out)
	}
	if got := queryCSV(t, root, "SELECT key, value FROM bronze.mlflow_metrics"); got != "key,value\nauc,0.91" {
		t.Fatalf("bronze.mlflow_metrics: %q", got)
	}
	if got := queryCSV(t, root, "SELECT e.name, r.status, p.key FROM bronze.mlflow_runs r JOIN bronze.mlflow_experiments e USING (experiment_id) JOIN bronze.mlflow_params p USING (run_uuid)"); got != "name,status,key\ndemo,FINISHED,alpha" {
		t.Fatalf("bronze join: %q", got)
	}
	if out := mustDuct(t, "mlflow", "sync", "--project", root); !strings.Contains(out, "experiments=0 runs=0 params=0 tags=0 metrics=0") {
		t.Fatalf("second sync must copy nothing: %q", out)
	}
	snap := queryCSV(t, root, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")
	mustDuct(t, "mlflow", "sync", "--project", root)
	if got := queryCSV(t, root, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"); got != snap {
		t.Fatalf("a no-op sync committed a snapshot: %s -> %s", snap, got)
	}

	// New run with 3 metric points and a tag, plus a run left RUNNING.
	runPy(t, root, py, addr, `
import mlflow
from mlflow import MlflowClient
mlflow.set_experiment("demo")
with mlflow.start_run():
    mlflow.log_param("alpha", 0.7)
    mlflow.set_tag("team", "a")
    for step, v in enumerate([0.5, 0.6, 0.7]):
        mlflow.log_metric("loss", v, step=step)
c = MlflowClient()
r = c.create_run(mlflow.get_experiment_by_name("demo").experiment_id, run_name="long")
c.log_param(r.info.run_id, "alpha", 0.9)
open("run_id.txt", "w").write(r.info.run_id)
`)
	out = mustDuct(t, "mlflow", "sync", "--project", root)
	if !strings.Contains(out, "runs=2") || !strings.Contains(out, "metrics=3") || !strings.Contains(out, "tags=") {
		t.Fatalf("incremental sync: %q", out)
	}
	if got := queryCSV(t, root, "SELECT count(*) FROM bronze.mlflow_runs"); got != "count_star()\n3" {
		t.Fatalf("runs after incremental sync: %q", got)
	}
	// The long run finishes and a tag changes: runs is updated in place, not duplicated.
	runPy(t, root, py, addr, `
from mlflow import MlflowClient
c = MlflowClient()
rid = open("run_id.txt").read()
c.set_tag(rid, "team", "b")
c.log_metric(rid, "auc", 0.99)
c.set_terminated(rid, "FINISHED")
`)
	mustDuct(t, "mlflow", "sync", "--project", root)
	if got := queryCSV(t, root, "SELECT status, count(*) AS n FROM bronze.mlflow_runs GROUP BY status ORDER BY status"); got != "status,n\nFINISHED,3" {
		t.Fatalf("run statuses: %q", got)
	}
	if got := queryCSV(t, root, "SELECT value FROM bronze.mlflow_tags WHERE key = 'team' ORDER BY value"); got != "value\na\nb" {
		t.Fatalf("tags: %q", got)
	}
	if got := queryCSV(t, root, "SELECT count(*) FROM bronze.mlflow_metrics"); got != "count_star()\n5" {
		t.Fatalf("metrics: %q", got)
	}
	// Identical to the source, and --full repeats nothing.
	if got := queryCSV(t, root, "SELECT (SELECT count(*) FROM mlflow.metrics) = (SELECT count(*) FROM bronze.mlflow_metrics) AND (SELECT count(*) FROM mlflow.runs) = (SELECT count(*) FROM bronze.mlflow_runs) AS same"); got != "same\ntrue" {
		t.Fatalf("bronze differs from mlflow: %q", got)
	}
	if out := mustDuct(t, "mlflow", "sync", "--full", "--project", root); !strings.Contains(out, "experiments=0 runs=0 params=0 tags=0 metrics=0") {
		t.Fatalf("--full on a synced lake: %q", out)
	}
}

// syncBuf is a goroutine-safe buffer.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// mlflowServeStage: `duct mlflow serve-model` + the proxy of `duct serve` with the real
// `mlflow models serve`: 200 with the right role, 401 without a token, 403 with another
// role, and the periodic sync of `[mlflow] sync = true` inside serve.
func mlflowServeStage(t *testing.T, root, py, addr string) {
	t.Helper()
	port := freePort(t)
	port = port[strings.LastIndex(port, ":")+1:]
	cfg, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	writeFile(t, filepath.Join(root, "duct.toml"), string(cfg)+"\nsync = true\nsync_interval = \"10s\"\n[mlflow.serving]\nroles = [\"scoring\"]\n[mlflow.serving.models.churn]\nport = "+port+"\n")
	mustDuct(t, "role", "create", "scoring", "--project", root)
	mustDuct(t, "role", "create", "otro", "--project", root)
	mustDuct(t, "user", "create", "sam", "--role", "scoring", "--password", "pw-sam-123", "--project", root)
	mustDuct(t, "user", "create", "ota", "--role", "otro", "--password", "pw-ota-123", "--project", root)
	sam := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "sam", "--project", root))
	ota := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "ota", "--project", root))

	ctx, cancel := context.WithCancel(context.Background())
	var out, errb syncBuf
	done := make(chan error, 1)
	go func() {
		done <- cmdMLflow(ctx, []string{"serve-model", "models:/churn/1", "--project", root}, &out, &errb)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Error("serve-model did not stop")
		}
	}()
	for deadline := time.Now().Add(3 * time.Minute); !strings.Contains(out.String(), "ready on"); time.Sleep(500 * time.Millisecond) {
		select {
		case err := <-done:
			t.Fatalf("serve-model exited: %v\n%s", err, errb.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("model not ready:\n%s\n%s", out.String(), errb.String())
		}
	}
	// The model server is on loopback only.
	if resp, err := http.Get("http://127.0.0.1:" + port + "/ping"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("direct /ping: %v", err)
	} else {
		_ = resp.Body.Close()
	}

	srv := startServe(t, root, nil)
	const inv = "/models/churn/invocations"
	body := `{"dataframe_split":{"columns":["tenure","spend"],"data":[[1,2],[3,4]]}}`
	if code, _ := callModel(t, srv.http, "POST", inv, "", body); code != 401 {
		t.Errorf("without token: %d, want 401", code)
	}
	if code, _ := callModel(t, srv.http, "POST", inv, ota, body); code != 403 {
		t.Errorf("wrong role: %d, want 403", code)
	}
	code, resp := callModel(t, srv.http, "POST", inv, sam, body)
	if code != 200 || !strings.Contains(resp, "4.0") || !strings.Contains(resp, "10.0") {
		t.Errorf("invoke: %d %s", code, resp)
	}
	if code, _ := callModel(t, srv.http, "GET", "/models/churn/health", sam, ""); code != 200 {
		t.Errorf("health: %d", code)
	}

	// [mlflow] sync = true: a run logged now reaches Bronze without running `duct mlflow sync`.
	before := queryCSV(t, root, "SELECT count(*) FROM bronze.mlflow_runs")
	runPy(t, root, py, addr, `
import mlflow
mlflow.set_experiment("demo")
with mlflow.start_run():
    mlflow.log_metric("auc", 0.5)
`)
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(2 * time.Second) {
		if queryCSV(t, root, "SELECT count(*) FROM bronze.mlflow_runs") != before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("serve did not sync the new run to bronze in 90 s")
		}
	}
	srv.stop()
}
