package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
)

// parseInterleaved parses flags that may appear before, between or after the
// positional arguments (duct grant select on x to y --project p).
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return nil, err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		pos, rest = append(pos, rest[0]), rest[1:]
	}
	return pos, nil
}

// govSession opens an unlocked engine over the project with the Duct tables
// migrated, returning the store and a cleanup func.
func govSession(ctx context.Context, dir string) (*governance.Store, contract.Engine, func(), error) {
	p, err := findProject(dir)
	if err != nil {
		return nil, nil, nil, err
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return nil, nil, nil, err
	}
	if _, err := project.Migrate(ctx, eng); err != nil {
		_ = eng.Close()
		return nil, nil, nil, err
	}
	return governance.NewStore(eng), eng, func() { _ = eng.Close() }, nil
}

func regen(ctx context.Context, st *governance.Store, stdout io.Writer) error {
	rep, err := st.Regenerate(ctx)
	if err != nil {
		return fmt.Errorf("regenerate role views: %w", err)
	}
	if rep.Roles > 0 || rep.Dropped > 0 {
		_, _ = fmt.Fprintf(stdout, "role views: %d roles, %d views, %d dropped\n", rep.Roles, rep.Views, rep.Dropped)
	}
	for _, s := range rep.Skipped {
		_, _ = fmt.Fprintf(stdout, "warning: granted relation no longer exists (%s)\n", s)
	}
	return nil
}

// cmdRole is `duct role create|list|drop`.
func cmdRole(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("role", stderr)
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("role needs a subcommand: create <name> | list | drop <name>")
	}
	st, _, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	switch pos[0] {
	case "create", "drop":
		if len(pos) != 2 {
			return usageErr("role %s <name>", pos[0])
		}
		if pos[0] == "create" {
			err = st.CreateRole(ctx, pos[1])
		} else {
			err = st.DropRole(ctx, pos[1])
		}
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "role %s: %s\n", pos[1], map[string]string{"create": "created", "drop": "dropped"}[pos[0]])
		return regen(ctx, st, stdout)
	case "list":
		roles, err := st.Roles(ctx)
		if err != nil {
			return err
		}
		for _, r := range roles {
			_, _ = fmt.Fprintf(stdout, "%s\t%d users\tschema %s\n", r.Name, r.Users, contract.RoleSchema(r.Name))
			for _, g := range r.Grants {
				_, _ = fmt.Fprintf(stdout, "  select on %s", g)
				if f := r.Filters[g]; f != "" {
					_, _ = fmt.Fprintf(stdout, "  where %s", f)
				}
				for k, e := range r.Masks {
					if strings.HasPrefix(k, g+".") {
						_, _ = fmt.Fprintf(stdout, "  mask %s = %s", strings.TrimPrefix(k, g+"."), e)
					}
				}
				_, _ = fmt.Fprintln(stdout)
			}
		}
		return nil
	}
	return usageErr("unknown role subcommand %q", pos[0])
}

func readPassword(inline string, fromStdin bool) (string, error) {
	switch {
	case inline != "" && fromStdin:
		return "", usageErr("use either --password or --password-stdin")
	case inline != "":
		return inline, nil
	case fromStdin:
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && (!errors.Is(err, io.EOF) || line == "") {
			return "", fmt.Errorf("read password: %w", err)
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
	return "", usageErr("a password is required: --password-stdin (preferred) or --password")
}

// cmdUser is `duct user create|list|passwd|drop|disable|enable`.
func cmdUser(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("user", stderr)
	role := fs.String("role", "", "role of the new user")
	password := fs.String("password", "", "password (visible in shell history; prefer --password-stdin)")
	stdin := fs.Bool("password-stdin", false, "read the password from standard input")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("user needs a subcommand: create <name> --role r | list | passwd <name> | drop <name> | disable <name> | enable <name>")
	}
	if pos[0] != "list" && len(pos) != 2 {
		return usageErr("user %s <name>", pos[0])
	}
	st, _, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	switch pos[0] {
	case "create":
		if *role == "" {
			return usageErr("user create needs --role")
		}
		pw, perr := readPassword(*password, *stdin)
		if perr != nil {
			return perr
		}
		err = st.CreateUser(ctx, pos[1], *role, pw)
	case "passwd":
		pw, perr := readPassword(*password, *stdin)
		if perr != nil {
			return perr
		}
		err = st.SetPassword(ctx, pos[1], pw)
	case "drop":
		err = st.DropUser(ctx, pos[1])
	case "disable", "enable":
		err = st.SetDisabled(ctx, pos[1], pos[0] == "disable")
	case "list":
		users, err := st.Users(ctx)
		if err != nil {
			return err
		}
		for _, u := range users {
			state := "active"
			if u.Disabled {
				state = "disabled"
			}
			_, _ = fmt.Fprintf(stdout, "%s\t%s\t%s\n", u.Name, u.Role, state)
		}
		return nil
	default:
		return usageErr("unknown user subcommand %q", pos[0])
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "user %s: %s done\n", pos[1], pos[0])
	return nil
}

// cmdGrant is `duct grant select on <schema.table> to <role>`; revoke is the
// same with "from".
func cmdGrant(ctx context.Context, verb string, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags(verb, stderr)
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	prep := map[string]string{"grant": "to", "revoke": "from"}[verb]
	if len(pos) != 5 || !strings.EqualFold(pos[0], "select") || !strings.EqualFold(pos[1], "on") || !strings.EqualFold(pos[3], prep) {
		return usageErr("duct %s select on <schema.table> %s <role>", verb, prep)
	}
	st, _, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	if verb == "grant" {
		err = st.Grant(ctx, pos[4], pos[2])
	} else {
		err = st.Revoke(ctx, pos[4], pos[2])
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "%s select on %s %s %s\n", verb, pos[2], prep, pos[4])
	return regen(ctx, st, stdout)
}

// cmdPolicy is `duct policy row-filter|mask|sync`.
func cmdPolicy(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("policy", stderr)
	role := fs.String("role", "", "role the policy applies to")
	where := fs.String("where", "", "row filter: SQL boolean expression over the table's columns")
	expr := fs.String("expr", "", "mask: SQL expression that replaces the column")
	drop := fs.Bool("drop", false, "remove the policy")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("policy needs a subcommand: row-filter <schema.table> --role r --where \"...\" | mask <schema.table.column> --role r --expr \"...\" | sync")
	}
	if pos[0] == "sync" {
		st, _, done, err := govSession(ctx, *dir)
		if err != nil {
			return err
		}
		defer done()
		return regen(ctx, st, stdout)
	}
	if len(pos) != 2 || *role == "" {
		return usageErr("policy %s <target> --role <role> ...", pos[0])
	}
	st, _, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	switch pos[0] {
	case "row-filter":
		if (*where == "") == !*drop {
			return usageErr("row-filter needs --where \"...\" or --drop")
		}
		err = st.SetRowFilter(ctx, *role, pos[1], *where)
	case "mask":
		if (*expr == "") == !*drop {
			return usageErr("mask needs --expr \"...\" or --drop")
		}
		i := strings.LastIndex(pos[1], ".")
		if i < 0 {
			return usageErr("mask target is <schema.table.column>")
		}
		err = st.SetMask(ctx, *role, pos[1][:i], pos[1][i+1:], *expr)
	default:
		return usageErr("unknown policy subcommand %q", pos[0])
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "policy %s on %s for role %s: ok\n", pos[0], pos[1], *role)
	return regen(ctx, st, stdout)
}

// cmdToken is `duct token create --user u [--label l] [--ttl d]` or `token revoke <id>`.
func cmdToken(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("token", stderr)
	user := fs.String("user", "", "user the token authenticates as")
	label := fs.String("label", "", "free-form note")
	ttl := fs.Duration("ttl", 0, "lifetime (default: no expiry)")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("token needs a subcommand: create --user u | revoke <id>")
	}
	st, _, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	switch pos[0] {
	case "create":
		if *user == "" {
			return usageErr("token create needs --user")
		}
		tok, id, err := st.CreateToken(ctx, *user, *label, *ttl)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "%s\n", tok)
		_, _ = fmt.Fprintf(stderr, "token id %s (shown once; expires: %s)\n", id, expiryText(*ttl))
		return nil
	case "revoke":
		if len(pos) != 2 {
			return usageErr("token revoke <id>")
		}
		if err := st.RevokeToken(ctx, pos[1]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "token %s revoked\n", pos[1])
		return nil
	}
	return usageErr("unknown token subcommand %q", pos[0])
}

func expiryText(ttl time.Duration) string {
	if ttl <= 0 {
		return "never"
	}
	return time.Now().Add(ttl).UTC().Format(time.RFC3339)
}
