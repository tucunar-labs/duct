package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/sandbox"
	"github.com/alonsoburon/duct/core/notebooks"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/udf"
)

const notebooksDir = "notebooks"

// notebookConfig fills the keys `duct serve` owns for the notebook protocol.
func notebookConfig(p *project.Project, cfg contract.Config) {
	if _, ok := cfg["dir"]; !ok {
		cfg["dir"] = filepath.Join(p.Root, notebooksDir)
	}
	if _, ok := cfg["python"]; !ok {
		cfg["python"] = p.PythonCommand()
	}
	// The protocol's Python app runs in bubblewrap like the console's notebooks ([sandbox] notebooks). An unusable
	// explicit "bwrap" stays "bwrap", so Serve refuses to start instead of running unisolated.
	if _, ok := cfg["sandbox"]; !ok {
		mode, auto, err := sandboxMode(p, project.SandboxNotebooks)
		if err != nil {
			mode = sandbox.Bwrap
		}
		cfg["sandbox"], cfg["sandbox_auto"] = mode, auto
	}
	// One identity runs every visitor's notebooks: an editor of a lower role than that identity could plant code
	// that runs with its rights (the console's "least privilege of the authors" rule does not apply here).
	role, _ := cfg["role"].(string)
	if role == "" {
		role = "duct_admin"
	}
	for _, r := range p.Config.Notebooks.EditorRoles() {
		if r != role && role == "duct_admin" {
			slog.Warn("notebook protocol: role " + r + " can edit notebooks (console) that this protocol runs as " + role + ": its code would run with those rights; restrict [notebooks] editors or the protocol's role")
			break
		}
	}
}

// initOrder is the order protocols are Init-ed in: the notebook one last, since
// it needs the address of the HTTP protocol.
func initOrder(names []string) []int {
	var first, last []int
	for i, n := range names {
		if n == "notebook" {
			last = append(last, i)
		} else {
			first = append(first, i)
		}
	}
	return append(first, last...)
}

// notebookEnvPassthrough are variables a local notebook session needs to open
// a browser or find its tools; nothing that carries credentials.
var notebookEnvPassthrough = []string{"BROWSER", "DISPLAY", "WAYLAND_DISPLAY", "XAUTHORITY", "TERM", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS"}

// notebookEnv is the environment of the notebook process: allow-listed, plus
// DUCT_URL and DUCT_TOKEN. Never the lake or catalog credentials of this process.
func notebookEnv(url, token string) []string {
	env := udf.SafeEnv(map[string]string{"DUCT_URL": url, "DUCT_TOKEN": token})
	for _, k := range notebookEnvPassthrough {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// cmdNotebook is `duct notebook [file.py] [flags] [-- args...]`: marimo (edit mode,
// on 127.0.0.1) with DUCT_URL and DUCT_TOKEN of the current user. With --script
// the notebook runs as a script (`python nb.py args`, as the orchestrator does).
//
// The editor and the script run as the invoking developer's own session: the process is not sandboxed (marimo needs
// the browser, the home directory and every file of the project), because whoever runs the CLI already owns the
// project directory, `.duct/` and the lake; there is no privilege boundary to defend. The notebooks of the web
// console and the job tasks, which run for other users, are sandboxed.
//
// `duct notebook approve <name> [--user U]` records that U (default: the owner of DUCT_JOBS_TOKEN / DUCT_TOKEN)
// reviewed the current content of notebooks/<name>.py, so a job running as U may run it again after someone
// else changed it (ADR 0006, "Menor privilegio de los autores").
func cmdNotebook(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "approve" {
		return cmdNotebookApprove(ctx, args[1:], stdout, stderr)
	}
	fs, dir := newFlags("notebook", stderr)
	user := fs.String("user", os.Getenv("DUCT_NOTEBOOK_USER"), "Duct user the notebook queries as (a token is minted for it and revoked on exit)")
	url := fs.String("url", os.Getenv("DUCT_URL"), "use an existing duct serve (HTTP URL) instead of starting one; the token comes from --token or DUCT_TOKEN")
	token := fs.String("token", os.Getenv("DUCT_TOKEN"), "token for --url")
	port := fs.Int("port", 0, "marimo port (default: marimo's)")
	headless := fs.Bool("headless", false, "do not open a browser")
	script := fs.Bool("script", false, "run the notebook as a script (python nb.py [args after --]) instead of the editor")
	ttl := fs.Duration("ttl", 12*time.Hour, "lifetime of the minted token")
	var extra []string // for the notebook, after --
	for i, a := range args {
		if a == "--" {
			args, extra = args[:i], args[i+1:]
			break
		}
	}
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	var file string
	if len(pos) > 1 {
		return usageErr("notebook takes one file (arguments for the notebook go after --)")
	}
	if len(pos) == 1 {
		file = pos[0]
	}
	if *script && file == "" {
		return usageErr("--script needs the notebook file")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if file != "" && !filepath.IsAbs(file) {
		if _, err := os.Stat(file); err != nil {
			file = filepath.Join(p.Root, file)
		} else if file, err = filepath.Abs(file); err != nil {
			return err
		}
	}

	connURL, connToken := *url, *token
	if connURL == "" {
		if *user == "" {
			return usageErr("notebook needs a way to reach the lake: --user <duct user> (starts a private duct serve on 127.0.0.1), or --url/--token (or DUCT_URL/DUCT_TOKEN) of a running one")
		}
		u, tok, stop, err := startEmbedded(ctx, p, *user, *ttl)
		if err != nil {
			return err
		}
		defer stop()
		connURL, connToken = u, tok
	} else if connToken == "" {
		_, _ = fmt.Fprintln(stderr, "warning: no token for --url; queries will be rejected (set DUCT_TOKEN)")
	}

	py, err := preparePython(ctx, p, []pyNeed{{"marimo", []string{"marimo"}}, {"duct", []string{p.PythonSDK()}}}, stderr)
	if err != nil {
		return err
	}
	var argv []string
	switch {
	case *script:
		argv = append(append(append([]string{}, py...), file), extra...)
	default:
		argv = append(append([]string{}, py...), "-m", "marimo", "edit", "--host", "127.0.0.1")
		if *port > 0 {
			argv = append(argv, "--port", strconv.Itoa(*port))
		}
		if *headless {
			argv = append(argv, "--headless")
		}
		if file != "" {
			argv = append(argv, file)
		}
		if len(extra) > 0 {
			argv = append(argv, "--")
			argv = append(argv, extra...)
		}
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // interpreter from duct.toml / the managed venv
	cmd.Dir = p.Root
	cmd.Env = notebookEnv(connURL, connToken)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	err = cmd.Run()
	if ctx.Err() != nil { // Ctrl-C is a normal way out of the editor
		return nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return fmt.Errorf("%s exited with status %d", filepath.Base(argv[0]), ee.ExitCode())
	}
	return err
}

// startEmbedded runs a private `duct serve` (HTTP protocol only, loopback, no
// jobs or streams) for the notebook and mints a token for user. stop revokes
// the token and shuts the server down.
func startEmbedded(ctx context.Context, p *project.Project, user string, ttl time.Duration) (url, token string, stop func(), err error) {
	st, _, done, err := govSession(ctx, p.Root)
	if err != nil {
		return "", "", nil, err
	}
	defer done()
	token, id, err := st.CreateToken(ctx, user, "duct notebook", ttl)
	if err != nil {
		return "", "", nil, err
	}
	sctx, cancel := context.WithCancel(ctx)
	ready := make(chan map[string]string, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- serve(sctx, p, serveOptions{
			Protocols: []string{"http"}, Listen: map[string]string{"http": "127.0.0.1:0"},
			NoJobs: true, NoStream: true, Ready: func(a map[string]string) { ready <- a },
		})
	}()
	revoke := func() {
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer rcancel()
		if st2, _, done2, err := govSession(rctx, p.Root); err == nil {
			_ = st2.RevokeToken(rctx, id)
			done2()
		}
	}
	select {
	case a := <-ready:
		stopFn := func() {
			cancel()
			<-finished
			revoke()
		}
		host := strings.Replace(a["http"], "[::]", "127.0.0.1", 1)
		return "http://" + host, token, stopFn, nil
	case e := <-finished:
		cancel()
		revoke()
		return "", "", nil, fmt.Errorf("embedded serve: %w", e)
	case <-time.After(2 * time.Minute):
		cancel()
		<-finished
		revoke()
		return "", "", nil, errors.New("embedded serve did not start")
	}
}

// cmdNotebookApprove is `duct notebook approve <name> [--user U]`.
func cmdNotebookApprove(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("notebook approve", stderr)
	user := fs.String("user", "", "Duct user the approval is for (default: the owner of DUCT_JOBS_TOKEN or DUCT_TOKEN)")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr("notebook approve needs one notebook name")
	}
	name := strings.TrimSuffix(filepath.Base(pos[0]), ".py")
	if !notebooks.ValidName(name) {
		return usageErr("%q is not a notebook name", pos[0])
	}
	st, eng, done, err := govSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer done()
	if *user == "" {
		tok := firstNonEmpty(os.Getenv("DUCT_JOBS_TOKEN"), os.Getenv("DUCT_TOKEN"))
		if tok == "" {
			return usageErr("notebook approve needs --user (or DUCT_JOBS_TOKEN / DUCT_TOKEN)")
		}
		id, err := st.Authenticate(ctx, contract.Credentials{Scheme: "bearer", Secret: tok})
		if err != nil {
			return fmt.Errorf("the token is not valid: %w", err)
		}
		*user = id.User
	} else if _, ok, err := st.UserByName(ctx, *user); err != nil || !ok {
		return fmt.Errorf("unknown user %q", *user)
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(p.Root, notebooksDir, name+".py"))
	if err != nil {
		return fmt.Errorf("notebook %s: %w", name, err)
	}
	hash := notebooks.HashOf(b)
	start := time.Now()
	err = notebooks.Policy{Engine: eng}.Trust(ctx, *user, name, hash, start)
	if ca := newCLIAudit(p, eng); ca != nil {
		ca.record("NOTEBOOK approve name="+name+" user="+*user+" hash="+hash[:12], start, 0, err)
		ca.close()
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "%s reviewed notebooks/%s.py (%s): jobs running as %s may run it until it changes\n", *user, name, hash[:12], *user)
	return nil
}
