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
	stop()
}
