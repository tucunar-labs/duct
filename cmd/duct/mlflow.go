package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/udf"
)

// mlflowAlias is the catalog name the MLflow database is attached under.
const mlflowAlias = "mlflow"

// cmdEnable is `duct enable mlflow`: writes the [mlflow] table to duct.toml (and, with a
// Postgres catalog, creates the mlflow schema). Nothing is installed or started.
func cmdEnable(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("enable", stderr)
	listen := fs.String("listen", "127.0.0.1:5000", "address of the MLflow server (loopback only)")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 || pos[0] != "mlflow" {
		return usageErr("enable needs a feature: duct enable mlflow")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if p.Config.MLflow != nil {
		_, _ = fmt.Fprintln(stdout, "mlflow is already enabled (see [mlflow] in duct.toml)")
		return nil
	}
	p.Config.MLflow = &project.MLflowSettings{Listen: *listen, Requirement: project.DefaultMLflowRequirement}
	if _, err := p.MLflowListen(); err != nil {
		return usageErr("%v", err)
	}
	if p.CatalogIsPostgres() {
		if err := createMLflowSchema(ctx, p); err != nil {
			return err
		}
	}
	path := filepath.Join(p.Root, project.ConfigFile)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, werr := fmt.Fprintf(f, "\n[mlflow]\nlisten      = %q\nrequirement = %q\n", *listen, project.DefaultMLflowRequirement)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return werr
	}
	store := "SQLite " + project.MLflowFile
	if p.CatalogIsPostgres() {
		store = "schema " + project.MLflowSchema + " of the catalog database"
	}
	_, _ = fmt.Fprintf(stdout, "MLflow enabled: metadata in %s, artifacts in %s\nNext: duct mlflow   (installs %s on first run; then `duct query \"SELECT * FROM mlflow.runs\"`)\n",
		store, p.MLflowArtifactsDestination(), strings.Join(p.MLflowRequirement(), " "))
	return nil
}

// createMLflowSchema creates schema mlflow in the catalog database (idempotent).
func createMLflowSchema(ctx context.Context, p *project.Project) error {
	dsn, err := p.CatalogDSN()
	if err != nil {
		return err
	}
	par, err := pgconn.Parse(dsn)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, par.Format(true))
	if err != nil {
		return fmt.Errorf("connect to the catalog: %s", par.Redact(err.Error()))
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+project.MLflowSchema); err != nil {
		return fmt.Errorf("create schema %s: %s", project.MLflowSchema, par.Redact(err.Error()))
	}
	return nil
}

// cmdMLflow is `duct mlflow`: `mlflow server` on loopback with the metadata in the
// catalog database and the artifacts under <lake>/_mlflow.
func cmdMLflow(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "sync":
			return cmdMLflowSync(ctx, args[1:], stdout, stderr)
		case "serve-model":
			return cmdServeModel(ctx, args[1:], stdout, stderr)
		default:
			return usageErr("unknown mlflow command %q (sync, serve-model; no command runs the MLflow server)", args[0])
		}
	}
	fs, dir := newFlags("mlflow", stderr)
	printOnly := fs.Bool("print", false, "print the command and the non-secret settings, do not start anything")
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
	listen, err := p.MLflowListen()
	if err != nil {
		return err
	}
	host, port, _ := net.SplitHostPort(listen)
	backend, err := p.MLflowBackendURI()
	if err != nil {
		return err
	}
	artifacts := p.MLflowArtifactsDestination()
	if !p.StorageIsS3() {
		if err := os.MkdirAll(strings.TrimPrefix(artifacts, "file://"), 0o755); err != nil {
			return err
		}
	}
	if p.CatalogIsPostgres() {
		if err := createMLflowSchema(ctx, p); err != nil {
			return err
		}
	}
	s3env, err := p.MLflowS3Env()
	if err != nil {
		return err
	}
	argv := []string{"-m", "mlflow", "server", "--host", host, "--port", port,
		"--artifacts-destination", artifacts, "--serve-artifacts"}
	if *printOnly {
		_, _ = fmt.Fprintf(stdout, "python %s\nbackend-store-uri: %s\nartifacts: %s\n", strings.Join(argv, " "), redactURI(backend), artifacts)
		return nil
	}
	need := []pyNeed{{"mlflow", p.MLflowRequirement()}, {"duct", []string{p.PythonSDK()}}}
	py, err := preparePython(ctx, p, need, stderr)
	if err != nil {
		return err
	}
	env := udf.SafeEnv(map[string]string{"MLFLOW_BACKEND_STORE_URI": backend}) // may hold a password: env, not argv
	for k, v := range s3env {
		env = append(env, k+"="+v)
	}
	cmd := exec.CommandContext(ctx, py[0], append(append([]string{}, py[1:]...), argv...)...) //nolint:gosec // interpreter from duct.toml / the managed venv
	cmd.Dir = p.Root
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 10 * time.Second
	_, _ = fmt.Fprintf(stdout, "MLflow on http://%s (tracking URI), artifacts in %s\n", listen, artifacts)
	if err := cmd.Run(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("mlflow server: %w", err)
	}
	return nil
}

func redactURI(u string) string {
	at := strings.LastIndex(u, "@")
	scheme := strings.Index(u, "://")
	if at < 0 || scheme < 0 || at < scheme {
		return u
	}
	return u[:scheme+3] + "***@" + u[at+1:]
}

// attachMLflow makes the MLflow metadata database readable as catalog "mlflow"
// (`SELECT * FROM mlflow.runs`), read only. It needs an unlocked engine (ATTACH is
// denied in locked mode, so roles of `duct serve` never see it) and does nothing
// when MLflow is not enabled or has not created its database yet.
func attachMLflow(ctx context.Context, p *project.Project, eng interface {
	Exec(context.Context, string, ...any) error
}) error {
	if p.Config.MLflow == nil {
		return nil
	}
	if p.CatalogIsPostgres() {
		dsn, err := p.CatalogDSN()
		if err != nil {
			return err
		}
		par, err := pgconn.Parse(dsn)
		if err != nil {
			return err
		}
		if err := eng.Exec(ctx, par.SecretSQL("duct_pg_mlflow")); err != nil {
			return fmt.Errorf("attach mlflow: %s", par.Redact(err.Error()))
		}
		q := fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET duct_pg_mlflow, SCHEMA %s, READ_ONLY)", mlflowAlias, project.MLflowSchema)
		if err := eng.Exec(ctx, q); err != nil {
			return fmt.Errorf("attach mlflow (has `duct mlflow` run once?): %s", par.Redact(err.Error()))
		}
		return nil
	}
	path := p.MLflowSQLitePath()
	if _, err := os.Stat(path); err != nil {
		return nil // `duct mlflow` has not run yet
	}
	return eng.Exec(ctx, fmt.Sprintf("ATTACH '%s' AS %s (TYPE sqlite, READ_ONLY)", strings.ReplaceAll(path, "'", "''"), mlflowAlias))
}
