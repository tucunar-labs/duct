package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
)

// cmdAudit is `duct audit query|prune|export` (ADR 0007).
func cmdAudit(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("audit", stderr)
	user := fs.String("user", "", "query: only this user")
	status := fs.String("status", "", "query: ok, error, denied or quota")
	since := fs.String("since", "", "query: only rows newer than this (1h, 30m, 7d)")
	limit := fs.Int("limit", 100, "query: at most this many rows (newest first)")
	format := fs.String("format", "table", "query: table or json")
	older := fs.String("older-than", "", "prune: delete rows older than this (30d); default [audit] retention")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr("audit needs a subcommand: query | prune | export")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	switch pos[0] {
	case "query":
		if *format != "table" && *format != "json" {
			return usageErr("--format must be table or json")
		}
		f := governance.AuditFilter{User: *user, Status: *status, Limit: *limit}
		if *since != "" {
			if f.Since, err = project.ParseAuditDuration(*since); err != nil {
				return usageErr("--since: %v", err)
			}
		}
		eng, err := openEngine(ctx, p, false)
		if err != nil {
			return err
		}
		defer func() { _ = eng.Close() }()
		rows, err := governance.QueryAudit(ctx, eng, f)
		if err != nil {
			return fmt.Errorf("%w (run `duct migrate` if the audit table is old)", err)
		}
		return writeAuditRows(stdout, rows, *format)
	case "prune":
		d := time.Duration(0)
		if *older != "" {
			if d, err = project.ParseAuditDuration(*older); err != nil {
				return usageErr("--older-than: %v", err)
			}
		} else if d, err = p.Config.Audit.RetentionDuration(); err != nil {
			return err
		}
		if d <= 0 {
			return usageErr("nothing to prune: pass --older-than 30d (retention is 0 = keep forever)")
		}
		eng, err := openEngine(ctx, p, false)
		if err != nil {
			return err
		}
		defer func() { _ = eng.Close() }()
		n, err := project.PruneAudit(ctx, eng, time.Now().Add(-d), 0)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "pruned %d audit rows older than %s\n", n, d)
		return nil
	case "export":
		eng, err := openEngine(ctx, p, false)
		if err != nil {
			return err
		}
		defer func() { _ = eng.Close() }()
		if _, err := project.Migrate(ctx, eng); err != nil {
			return err
		}
		n, err := governance.ExportAudit(ctx, eng)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "exported %d audit rows to %s\n", n, governance.AuditLogTable)
		return nil
	}
	return usageErr("unknown audit subcommand %q (query | prune | export)", pos[0])
}

func writeAuditRows(w io.Writer, rows []governance.AuditRow, format string) error {
	if format == "json" {
		if rows == nil {
			rows = []governance.AuditRow{}
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "WHEN\tUSER\tROLE\tPROTO\tSTATUS\tMS\tROWS\tHITS\tSQL")
	for _, r := range rows {
		st := r.Status
		if r.Cached {
			st += "*"
		}
		sql := strings.Join(strings.Fields(r.SQL), " ")
		if r.Error != "" && r.Status != "ok" {
			sql += "  -- " + r.Error
		}
		if len([]rune(sql)) > 90 {
			sql = string([]rune(sql)[:90]) + "…"
		}
		_, _ = fmt.Fprintf(tw, "%.19s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\n", r.At, r.User, r.Role, r.Protocol, st, r.DurationMS, r.Rows, r.Hits, sql)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "(%d rows; * = served from cache)\n", len(rows))
	return err
}

// cliAudit records the local CLI commands in the audit log ([audit] cli).
// The zero/nil value records nothing.
type cliAudit struct {
	rec *governance.Recorder
}

// newCLIAudit returns a recorder over eng, or nil when [audit] cli = false.
func newCLIAudit(p *project.Project, eng contract.Engine) *cliAudit {
	if !p.Config.Audit.CLIEnabled() {
		return nil
	}
	return &cliAudit{rec: governance.NewRecorder(eng)}
}

func (c *cliAudit) record(sql string, start time.Time, rows int64, err error) {
	if c != nil {
		c.rec.Record(sql, start, rows, err)
	}
}

func (c *cliAudit) close() {
	if c != nil {
		c.rec.Close()
	}
}

// countStream counts the rows that pass through a stream.
type countStream struct {
	contract.Stream
	rows int64
}

func (s *countStream) Next() bool {
	ok := s.Stream.Next()
	if ok {
		s.rows += s.Stream.RecordBatch().NumRows()
	}
	return ok
}

// auditedQuery runs sql on eng and writes the result, recording the
// statement (user = OS user, protocol cli) whether it worked or not.
func auditedQuery(ctx context.Context, p *project.Project, eng contract.Engine, sql, format string, out io.Writer) error {
	ca := newCLIAudit(p, eng)
	defer ca.close()
	start := time.Now()
	st, err := eng.Query(ctx, sql)
	if err != nil {
		ca.record(sql, start, 0, err)
		return err
	}
	cs := &countStream{Stream: st}
	defer cs.Release()
	err = writeStream(cs, format, out)
	ca.record(sql, start, cs.rows, err)
	return err
}
