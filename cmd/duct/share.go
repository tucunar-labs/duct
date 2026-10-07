package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/s3conn"
	"github.com/alonsoburon/duct/core/sharing"
)

const (
	sharingConfigKey     = "sharing"
	defaultSharingListen = "127.0.0.1:8124"
	sharingKeyFile       = "sharing.key"
	defaultRecipientTTL  = 90 * 24 * time.Hour
)

// sharesLocation is where the copies live. It must be outside the lake's data
// path, because `duct gc` deletes every file there that DuckLake does not know.
// Local lake: <project>/shares. s3://bucket/prefix lake: s3://bucket/prefix.shares
// (a lake at the bucket root needs [serve.sharing] storage).
func sharesLocation(p *project.Project) (string, error) {
	if v, _ := p.Config.Serve.Modules[sharingConfigKey]["storage"].(string); v != "" {
		if sharing.Overlaps(v, p.DataPath()) {
			return "", fmt.Errorf("[serve.sharing] storage %q overlaps the lake (%s): `duct gc` would delete the copies", v, p.DataPath())
		}
		return v, nil
	}
	if !p.StorageIsS3() {
		return filepath.Join(p.Root, "shares"), nil
	}
	bucket, prefix, err := s3conn.ParseStorage(p.Config.Storage)
	if err != nil {
		return "", err
	}
	if prefix == "" {
		return "", errors.New("the lake is the whole bucket: set [serve.sharing] storage to a location outside it (another bucket or prefix)")
	}
	return "s3://" + bucket + "/" + prefix + ".shares", nil
}

// sharingConfig fills the keys `duct serve` owns in the sharing module's config.
func sharingConfig(p *project.Project, cfg contract.Config) {
	if _, ok := cfg["storage"]; !ok {
		if loc, err := sharesLocation(p); err == nil {
			cfg["storage"] = loc
		}
	}
	if _, ok := cfg["signing_key_file"]; !ok {
		cfg["signing_key_file"] = filepath.Join(filepath.Dir(p.StatePath()), sharingKeyFile)
	}
}

// sharingEndpoint is the URL recipients put in their profile.
func sharingEndpoint(p *project.Project) string {
	m := p.Config.Serve.Modules[sharingConfigKey]
	if v, _ := m["public_url"].(string); v != "" {
		return strings.TrimSuffix(v, "/") + "/delta-sharing"
	}
	listen, _ := m["listen"].(string)
	if listen == "" {
		listen = defaultSharingListen
	}
	scheme := "http"
	if c, _ := m["tls_cert"].(string); c != "" {
		scheme = "https"
	}
	return scheme + "://" + listen + "/delta-sharing"
}

// openSharesStorage opens the copies' storage for writing and deleting (no URLs).
func openSharesStorage(p *project.Project) (sharing.Storage, error) {
	loc, err := sharesLocation(p)
	if err != nil {
		return nil, err
	}
	spec := sharing.StorageSpec{Location: loc}
	spec.S3PublicEndpoint, _ = p.Config.Serve.Modules[sharingConfigKey]["s3_public_endpoint"].(string)
	if cfg, ok, err := p.S3(); err != nil {
		return nil, err
	} else if ok {
		spec.S3 = &cfg
	}
	return sharing.OpenStorage(spec)
}

// parseTTL reads 90d, 12h, 30m... ("0" or "never": no expiry).
func parseTTL(s string) (time.Duration, error) {
	switch s {
	case "", "default":
		return defaultRecipientTTL, nil
	case "0", "never":
		return 0, nil
	}
	if n, ok := strings.CutSuffix(s, "d"); ok {
		days, err := strconv.Atoi(n)
		if err != nil || days < 0 {
			return 0, fmt.Errorf("--ttl %q: use 90d, 12h, 30m, or 0 for no expiry", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("--ttl %q: use 90d, 12h, 30m, or 0 for no expiry", s)
	}
	return d, nil
}

type maskFlags map[string]string

func (m maskFlags) String() string { return fmt.Sprint(map[string]string(m)) }
func (m maskFlags) Set(v string) error {
	col, expr, ok := strings.Cut(v, "=")
	col = strings.TrimSpace(col)
	if !ok || col == "" || strings.TrimSpace(expr) == "" {
		return errors.New("use --mask column=expression")
	}
	m[strings.ToLower(col)] = strings.TrimSpace(expr)
	return nil
}

// shareSession opens the project, the engine (unlocked, Duct tables migrated)
// and the sharing store.
func shareSession(ctx context.Context, dir string) (*project.Project, *sharing.Store, contract.Engine, func(), error) {
	p, err := findProject(dir)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if _, err := project.Migrate(ctx, eng); err != nil {
		_ = eng.Close()
		return nil, nil, nil, nil, err
	}
	return p, sharing.NewStore(eng), eng, func() { _ = eng.Close() }, nil
}

// purge deletes the stored copies under prefixes (a revoked grant leaves none).
func purge(ctx context.Context, p *project.Project, prefixes []string) error {
	if len(prefixes) == 0 {
		return nil
	}
	st, err := openSharesStorage(p)
	if err != nil {
		return err
	}
	for _, pre := range prefixes {
		if err := sharing.DeletePrefix(ctx, st, pre); err != nil {
			return err
		}
	}
	return nil
}

func dropWord(pos []string, word string) []string {
	if len(pos) >= 3 && pos[2] == word {
		return append(append([]string{}, pos[:2]...), pos[3:]...)
	}
	return pos
}

// cmdShare is `duct share create|list|add|remove|drop|grant|revoke|refresh|gc`.
func cmdShare(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("share", stderr)
	as := fs.String("as", "", "add: name of the table inside the share (default: the table name)")
	pfilter := fs.String("partition-filter", "", "add: predicate every recipient's copy of the table must satisfy")
	table := fs.String("table", "", "grant: table (alias) the --row-filter / --mask apply to")
	rowFilter := fs.String("row-filter", "", "grant: predicate over the table's columns limiting the recipient's rows")
	masks := maskFlags{}
	fs.Var(masks, "mask", "grant: column=expression replacing a column for the recipient (repeatable)")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("share needs a subcommand: create <share> | list | add <share> <schema.table> | remove <share> <table> | drop <share> | grant <share> to <recipient> | revoke <share> from <recipient> | refresh [share] | gc")
	}
	p, st, eng, done, err := shareSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	gov := st.Governance()
	switch pos[0] {
	case "create":
		if len(pos) != 2 {
			return usageErr("share create <share>")
		}
		if err := st.CreateShare(ctx, pos[1]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "share %s created\n", pos[1])
		return nil
	case "add":
		if len(pos) != 3 {
			return usageErr("share add <share> <schema.table> [--as alias] [--partition-filter expr]")
		}
		if err := st.AddTable(ctx, pos[1], pos[2], *as, *pfilter); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "share %s: added %s\n", pos[1], pos[2])
		return regen(ctx, gov, stdout)
	case "remove":
		if len(pos) != 3 {
			return usageErr("share remove <share> <table>")
		}
		prefixes, err := st.RemoveTable(ctx, pos[1], pos[2])
		if err != nil {
			return err
		}
		if err := purge(ctx, p, prefixes); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "share %s: removed %s\n", pos[1], pos[2])
		return regen(ctx, gov, stdout)
	case "drop":
		if len(pos) != 2 {
			return usageErr("share drop <share>")
		}
		prefixes, err := st.DropShare(ctx, pos[1])
		if err != nil {
			return err
		}
		if err := purge(ctx, p, prefixes); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "share %s dropped\n", pos[1])
		return regen(ctx, gov, stdout)
	case "grant":
		pos = dropWord(pos, "to")
		if len(pos) != 3 {
			return usageErr("share grant <share> to <recipient> [--table t] [--row-filter expr] [--mask col=expr]")
		}
		if err := st.Grant(ctx, pos[1], pos[2], sharing.GrantOptions{Table: *table, RowFilter: *rowFilter, Masks: masks}); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "share %s granted to %s\n", pos[1], pos[2])
		return regen(ctx, gov, stdout)
	case "revoke":
		pos = dropWord(pos, "from")
		if len(pos) != 3 {
			return usageErr("share revoke <share> from <recipient>")
		}
		prefixes, err := st.Revoke(ctx, pos[1], pos[2])
		if err != nil {
			return err
		}
		if err := purge(ctx, p, prefixes); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "share %s revoked from %s (copies deleted)\n", pos[1], pos[2])
		return regen(ctx, gov, stdout)
	case "list":
		return shareList(ctx, st, stdout)
	case "refresh", "gc":
		files, err := openSharesStorage(p)
		if err != nil {
			return err
		}
		if _, err := gov.Regenerate(ctx); err != nil {
			return fmt.Errorf("regenerate role views: %w", err)
		}
		h := governance.NewHandler(eng, governance.HandlerOptions{Timeout: 30 * time.Minute})
		defer func() { _ = h.Close() }()
		svc := sharing.NewService(eng, h.ForProtocol("sharing"), files, sharing.Options{})
		if pos[0] == "gc" {
			n, err := svc.GC(ctx)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(stdout, "deleted %d superseded or orphaned files\n", n)
			return nil
		}
		share := ""
		if len(pos) > 1 {
			share = pos[1]
		}
		stats, err := svc.Refresh(ctx, share)
		_, _ = fmt.Fprintf(stdout, "checked %d copies, rewrote %d\n", stats.Checked, stats.Rewritten)
		return err
	}
	return usageErr("unknown share subcommand %q", pos[0])
}

func shareList(ctx context.Context, st *sharing.Store, stdout io.Writer) error {
	shares, err := st.Shares(ctx)
	if err != nil {
		return err
	}
	for _, s := range shares {
		_, _ = fmt.Fprintf(stdout, "%s\t%d tables\t%d recipients\n", s.Name, len(s.Tables), len(s.Recipients))
		for _, t := range s.Tables {
			_, _ = fmt.Fprintf(stdout, "  table %s.%s", t.Schema(), t.Alias)
			if t.Relation != t.Schema()+"."+t.Alias {
				_, _ = fmt.Fprintf(stdout, " (from %s)", t.Relation)
			}
			if t.PartitionFilter != "" {
				_, _ = fmt.Fprintf(stdout, "  where %s", t.PartitionFilter)
			}
			_, _ = fmt.Fprintln(stdout)
		}
		for _, r := range s.Recipients {
			_, _ = fmt.Fprintf(stdout, "  recipient %s\n", r)
			for _, t := range s.Tables {
				rf, ms, err := st.Policy(ctx, r, t.Relation)
				if err != nil {
					return err
				}
				if rf != "" {
					_, _ = fmt.Fprintf(stdout, "    %s: row filter %s\n", t.Alias, rf)
				}
				for _, m := range ms {
					_, _ = fmt.Fprintf(stdout, "    %s: mask %s\n", t.Alias, m)
				}
			}
		}
	}
	return nil
}

// cmdRecipient is `duct recipient create|list|token|revoke|drop`.
func cmdRecipient(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("recipient", stderr)
	ttl := fs.String("ttl", "", "token lifetime: 90d (default), 12h, ... or 0 for no expiry")
	endpoint := fs.String("endpoint", "", "profile endpoint (default: from [serve.sharing]: public_url or listen)")
	profile := fs.String("profile", "", "profile file to write (default <recipient>.share; - for standard output)")
	rotate := fs.Bool("rotate", false, "token: revoke the recipient's earlier tokens first")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("recipient needs a subcommand: create <name> | list | token <name> | revoke <name> | drop <name>")
	}
	p, st, _, done, err := shareSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	issue := func(name string, is sharing.Issued) error {
		ep := *endpoint
		if ep == "" {
			ep = sharingEndpoint(p)
		}
		prof := sharing.NewProfile(ep, is)
		out := *profile
		if out == "" {
			out = name + ".share"
		}
		if out == "-" {
			_, err := stdout.Write(prof.JSON())
			return err
		}
		if err := prof.WriteFile(out); err != nil {
			return err
		}
		exp := "never expires"
		if !is.ExpiresAt.IsZero() {
			exp = "expires " + is.ExpiresAt.Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(stdout, "profile for %s written to %s (%s, mode 600). It holds the token: hand it over by a safe channel.\n", name, out, exp)
		return nil
	}
	switch pos[0] {
	case "create", "token":
		if len(pos) != 2 {
			return usageErr("recipient %s <name> [--ttl 90d] [--endpoint url] [--profile file]", pos[0])
		}
		d, err := parseTTL(*ttl)
		if err != nil {
			return usageErr("%v", err)
		}
		var is sharing.Issued
		if pos[0] == "create" {
			if is, err = st.CreateRecipient(ctx, pos[1], d); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(stdout, "recipient %s created (role %s)\n", pos[1], sharing.RoleOf(pos[1]))
			if err := regen(ctx, st.Governance(), stdout); err != nil {
				return err
			}
		} else if is, err = st.NewToken(ctx, pos[1], d, *rotate); err != nil {
			return err
		}
		return issue(pos[1], is)
	case "list":
		rs, err := st.Recipients(ctx)
		if err != nil {
			return err
		}
		for _, r := range rs {
			state := "active"
			if r.Disabled {
				state = "revoked"
			}
			_, _ = fmt.Fprintf(stdout, "%s\t%s\trole %s\tshares: %s\n", r.Name, state, r.Role, strings.Join(r.Shares, ","))
		}
		return nil
	case "revoke":
		if len(pos) != 2 {
			return usageErr("recipient revoke <name>")
		}
		if err := st.RevokeRecipient(ctx, pos[1]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "recipient %s revoked: its tokens stop working now (file URLs already issued last at most %s)\n", pos[1], sharing.DefaultURLTTL)
		return nil
	case "drop":
		if len(pos) != 2 {
			return usageErr("recipient drop <name>")
		}
		prefixes, err := st.DropRecipient(ctx, pos[1])
		if err != nil {
			return err
		}
		if err := purge(ctx, p, prefixes); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "recipient %s dropped\n", pos[1])
		return regen(ctx, st.Governance(), stdout)
	}
	return usageErr("unknown recipient subcommand %q", pos[0])
}
