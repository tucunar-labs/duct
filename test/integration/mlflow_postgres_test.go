//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMLflowOnPostgresCatalog: with a Postgres catalog the MLflow metadata lives in schema
// mlflow of the same database (ADR 0010), the artifacts under <lake>/_mlflow, and
// `duct query "SELECT * FROM mlflow.runs"` reads it. Installs MLflow: needs uv and network.
func TestMLflowOnPostgresCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv is not installed")
	}
	pg := startPG(t)
	pg.createDB(t, "mlflowlake")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("mlflowlake"))
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	_, file, _, _ := runtime.Caller(0)
	sdk := filepath.Join(filepath.Dir(file), "..", "..", "python", "duct-data") + "[arrow,pg]"
	env := append([]string{"DUCT_SDK=" + sdk, "DUCT_PYTHON=", "MLFLOW_TRACKING_URI="}, pwEnv...)

	mustDuct(t, root, env, "enable", "mlflow", "--listen", addr)
	if got := pg.sql(t, "mlflowlake", "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'mlflow'"); got != "1" {
		t.Fatalf("schema mlflow not created: %s", got)
	}

	cmd := exec.Command(ductBinary(t), "mlflow")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), env...)
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGINT)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	for deadline := time.Now().Add(15 * time.Minute); ; time.Sleep(time.Second) {
		if resp, err := http.Get("http://" + addr + "/health"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("MLflow did not come up:\n%s", errb.String())
		}
	}

	py := filepath.Join(root, ".duct", "venv", "bin", "python")
	log := exec.Command(py, "-c", "import mlflow\nmlflow.set_experiment('pg')\nwith mlflow.start_run():\n    mlflow.log_param('a', 1)\n    mlflow.log_metric('auc', 0.8)\n    mlflow.log_text('hello', 'note.txt')\n")
	log.Env = append(os.Environ(), "MLFLOW_TRACKING_URI=http://"+addr)
	if b, err := log.CombinedOutput(); err != nil {
		t.Fatalf("log run: %v\n%s", err, b)
	}
	if got := csvQuery(t, root, pwEnv, "SELECT key, value FROM mlflow.metrics"); got != "key,value\nauc,0.8" {
		t.Fatalf("mlflow.metrics: %q", got)
	}
	if got := pg.sql(t, "mlflowlake", "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'mlflow' AND table_name = 'runs'"); got != "1" {
		t.Fatalf("runs not in schema mlflow: %s", got)
	}
	if got := pg.sql(t, "mlflowlake", "SELECT count(*) FROM information_schema.tables WHERE table_schema <> 'mlflow' AND table_name = 'runs'"); got != "0" {
		t.Fatalf("MLflow leaked tables outside its schema: %s", got)
	}
	if r := duct(t, root, pwEnv, "query", "DELETE FROM mlflow.runs"); r.err == nil {
		t.Fatal("mlflow must be read only")
	}
	// Sync to Bronze from the Postgres schema: copies the run, then nothing.
	if out := mustDuct(t, root, pwEnv, "mlflow", "sync"); !strings.Contains(out, "runs=1") || !strings.Contains(out, "metrics=1") {
		t.Fatalf("sync: %q", out)
	}
	if got := csvQuery(t, root, pwEnv, "SELECT key, value FROM bronze.mlflow_metrics"); got != "key,value\nauc,0.8" {
		t.Fatalf("bronze.mlflow_metrics: %q", got)
	}
	if out := mustDuct(t, root, pwEnv, "mlflow", "sync"); !strings.Contains(out, "runs=0 params=0 tags=0 metrics=0") {
		t.Fatalf("second sync: %q", out)
	}
	if _, err := os.Stat(filepath.Join(root, "lake", "_mlflow")); err != nil {
		t.Fatalf("artifacts folder: %v", err)
	}
}
