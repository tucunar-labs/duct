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
  catalog              catalog tools: info, migrate --to postgres://... (SQLite -> Postgres)
  gc                   checkpoint the catalog WAL and run DuckLake maintenance
  ingest [name...]     load ingest/*.yaml sources into bronze tables
  run [target...]      refresh pipelines/ tables in dependency order (--only)
  serve                serve the lake over every protocol (pg, http) as governed users
  role|user|token      manage roles, users and API tokens (duct role create analistas)
  grant|revoke         duct grant select on gold.revenue to analistas
  policy               row-filter / mask / sync of the per-role views
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
	case "init", "query", "sql", "migrate", "catalog", "gc", "run", "serve", "role", "user", "token", "grant", "revoke", "policy":
		return runProject(fs.Arg(0), fs.Args()[1:], stdout, stderr)
	case "ingest":
		return runIngest(fs.Args()[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "duct: unknown command %q\n", fs.Arg(0))
		fs.Usage()
		return 2
	}
	return 0
}
