// Package sourceodbc ingests from any ODBC data source (Oracle, HANA, DB2,
// SQL Server, Postgres...) with DuckDB's official odbc_scanner extension. It
// never touches the engine: it returns a SELECT over odbc_query(...)
// (contract.SQLSource) that the core executes and loads into Bronze.
package sourceodbc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindSource,
		Name: "odbc",
		New:  func() contract.Module { return &Source{} },
	})
}

// Source is the ODBC source module.
type Source struct {
	conn      string
	query     string
	watermark string
}

var _ contract.SQLSource = (*Source)(nil)

// Name implements contract.Module.
func (*Source) Name() string { return "odbc" }

var (
	envRe     = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
	tableRe   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$`)
	numberRe  = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
	handleVar = "duct_odbc"
	setupStmt = []string{"INSTALL odbc_scanner", "LOAD odbc_scanner"}
)

// Init reads the ingestion config: watermark and options {connection, table | query}.
// ${VAR} references in connection are replaced from the environment, so
// secrets stay out of the YAML.
func (s *Source) Init(_ context.Context, cfg contract.Config) error {
	s.watermark, _ = cfg["watermark"].(string)
	opts, _ := cfg["options"].(map[string]any)
	conn, _ := opts["connection"].(string)
	if conn == "" {
		return errors.New("source-odbc: options.connection is required (DSN=name or a connection string)")
	}
	var missing []string
	s.conn = envRe.ReplaceAllStringFunc(conn, func(m string) string {
		name := envRe.FindStringSubmatch(m)[1]
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
		}
		return v
	})
	if len(missing) > 0 {
		return fmt.Errorf("source-odbc: environment variable(s) not set: %s", strings.Join(missing, ", "))
	}
	table, _ := opts["table"].(string)
	query, _ := opts["query"].(string)
	switch {
	case table != "" && query != "":
		return errors.New("source-odbc: set options.table or options.query, not both")
	case table != "":
		if !tableRe.MatchString(table) {
			return fmt.Errorf("source-odbc: table %q must be [catalog.][schema.]name", table)
		}
		s.query = "SELECT * FROM " + table
	case query != "":
		s.query = strings.TrimRight(strings.TrimSpace(query), "; \n\t")
	default:
		return errors.New("source-odbc: options.table or options.query is required")
	}
	return nil
}

// Plan implements contract.SQLSource. The incremental filter is pushed down
// to the remote database: SELECT * FROM (<query>) duct_src WHERE col > <last>.
// The watermark literal is bare if it looks numeric, otherwise a quoted
// string the remote database converts (ISO timestamps work everywhere).
func (s *Source) Plan(_ context.Context, st contract.IngestState) (contract.SQLPlan, error) {
	remote := s.query
	if s.watermark != "" && st.Watermark != "" {
		lit := quote(st.Watermark)
		if numberRe.MatchString(st.Watermark) {
			lit = st.Watermark
		}
		remote = fmt.Sprintf("SELECT * FROM (%s) duct_src WHERE %s > %s", s.query, s.watermark, lit)
	}
	// Explicit connection (odbc_connect/odbc_close) instead of the implicit one
	// of odbc_query(conn_string, ...): the scanner frees that one before the
	// statement that used it, and unixODBC 2.3.12 (Ubuntu 24.04, Debian 12)
	// segfaults in SQLFreeStmt on the second query of a process. Here the
	// statement is destroyed first, then the connection is closed.
	return contract.SQLPlan{
		Setup:  setupStmt,
		Open:   []string{"SET VARIABLE " + handleVar + " = odbc_connect(" + quote(s.conn) + ")"},
		Select: fmt.Sprintf("SELECT * FROM odbc_query(getvariable('%s'), %s)", handleVar, quote(remote)),
		Close:  []string{"SELECT odbc_close(getvariable('" + handleVar + "'))"},
	}, nil
}

// Close implements contract.Module.
func (*Source) Close() error { return nil }

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
