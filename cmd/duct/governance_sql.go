package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/alonsoburon/duct/core/governance"
)

// cmdSQL is `duct sql "<statement>"`: Unity-Catalog-style governance
// statements (CREATE ROLE, GRANT, SHOW GRANTS, ALTER TABLE ... SET ROW FILTER
// / SET MASK ...; see core/governance/sql.go) run against the project, with the
// same effect as `duct role|grant|policy`. Any other statement goes to the
// engine, like `duct query`.
func cmdSQL(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("sql", stderr)
	format := fs.String("format", "table", "output format: table, csv or json")
	locked := fs.Bool("locked", false, "run non-governance SQL with the server restrictions")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr(`sql takes a single statement (quote it): duct sql "GRANT SELECT ON gold.ventas TO analistas"`)
	}
	sql := pos[0]
	if _, err := newWriter(*format, io.Discard); err != nil {
		return usageErr("%v", err)
	}
	stmt, perr := governance.Parse(sql)
	if perr != nil && !errors.Is(perr, governance.ErrNotGovernance) {
		return perr
	}
	if errors.Is(perr, governance.ErrNotGovernance) {
		p, err := findProject(*dir)
		if err != nil {
			return err
		}
		eng, err := openEngineUDF(ctx, p, *locked)
		if err != nil {
			return err
		}
		defer func() { _ = eng.Close() }()
		return auditedQuery(ctx, p, eng, sql, *format, stdout)
	}
	store, eng, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	ca := newCLIAudit(p, eng)
	defer ca.close()
	start := time.Now()
	res, err := store.Execute(ctx, stmt)
	ca.record(sql, start, 0, err)
	if err != nil {
		return err
	}
	if len(res.Columns) == 0 {
		_, err = fmt.Fprintln(stdout, res.Tag)
		return err
	}
	st, err := eng.Query(ctx, res.SQL())
	if err != nil {
		return err
	}
	defer st.Release()
	return writeStream(st, *format, stdout)
}
