// Command duct is the Duct CLI.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/alonsoburon/duct/core/registry"
)

// version is injected with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: duct <command>

commands:
  version              print the version
  modules              list registered modules
  init <dir>           create a project (duct.toml, lake/ or --storage s3://..., SQLite catalog)
  query "SQL"          run SQL against the lake (--format table|csv|json)
  sql "STATEMENT"      governance SQL (CREATE ROLE, GRANT, SHOW GRANTS, ALTER TABLE ... SET ROW FILTER|MASK) or any SQL
  migrate              apply Duct-owned table migrations
  backup --to X        consistent backup of catalog, state, project files and lake data (folder or s3://); backup list
  restore --from X DIR restore a backup into an empty folder and verify it (snapshot, row counts, files)
  catalog              catalog tools: info, migrate --to postgres://... (SQLite -> Postgres)
  gc                   checkpoint the catalog WAL and run DuckLake maintenance
  ingest [name...]     load ingest/*.yaml sources into bronze tables
  stream [name...]     land Kafka topics (streams/*.yaml) in bronze tables, committing offsets after each batch
  alerts [check]       evaluate alerts/*.sql over the last minutes of the streams and notify (ntfy, webhook, WhatsApp, PagerDuty)
  run [target...]      refresh pipelines/ tables in dependency order (--only)
  jobs <cmd>           jobs/*.yaml orchestration: list, run <job>, history (scheduler runs in serve)
  serve                serve the lake over every protocol (pg, http, mcp) as governed users
  role|user|token      manage roles, users and API tokens (duct role create analistas)
  contract <cmd>       data contracts (ODCS): lint, diff, test, compile, freshness
  grant|revoke         duct grant select on gold.revenue to analistas
  share|recipient      share tables with other organisations (OpenSharing): duct share create|add|grant|refresh, duct recipient create
  policy               row-filter / mask / sync of the per-role views
  semantic <cmd>       semantic layer (Ossie): validate, list, build, query
  functions <cmd>      UDFs of functions/ (Python @duct.function, executables): list, check, bench
  notebook [file]      marimo with the lake connected through duct serve (token for the current user)
  enable mlflow        turn on the optional MLflow server; "duct mlflow" runs it
  dashboard <cmd>      dashboards/*.yaml over the semantic layer: lint, list
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("duct", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, usage)
	}
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}
	switch fs.Arg(0) {
	case "version":
		_, _ = fmt.Fprintln(stdout, version)
	case "modules":
		for _, m := range registry.List("") {
			_, _ = fmt.Fprintf(stdout, "%s\t%s\n", m.Kind, m.Name)
		}
	case "init", "query", "sql", "migrate", "catalog", "gc", "run", "serve", "role", "user", "token", "grant", "revoke", "policy", "semantic", "share", "recipient", "functions", "notebook", "mlflow", "enable":
		return runProject(fs.Arg(0), fs.Args()[1:], stdout, stderr)
	case "backup", "restore":
		return runBackupCmd(fs.Arg(0), fs.Args()[1:], stdout, stderr)
	case "dashboard":
		return runDashboard(fs.Args()[1:], stdout, stderr)
	case "contract":
		return runContract(fs.Args()[1:], stdout, stderr)
	case "ingest":
		return runIngest(fs.Args()[1:], stdout, stderr)
	case "stream":
		return runStream(fs.Args()[1:], stdout, stderr)
	case "alerts":
		return runAlertsCmd(fs.Args()[1:], stdout, stderr)
	case "jobs":
		return runJobs(fs.Args()[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "duct: unknown command %q\n", fs.Arg(0))
		fs.Usage()
		return 2
	}
	return 0
}
