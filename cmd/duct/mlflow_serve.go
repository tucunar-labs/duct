package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/metrics"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/udf"
)

// Online serving (ADR 0010). `duct mlflow serve-model` runs `mlflow models serve` bound to
// 127.0.0.1 (MLflow has no authentication); `duct serve` is the only way in: it proxies
// /models/<name>/invocations (and /ping as /models/<name>/health) after authenticating the
// bearer token or password, checking [mlflow.serving] roles and auditing every call.

var modelRequests = metrics.Default().Counter("duct_model_requests_total", "Calls to /models/<name>/ by model and outcome (ok, error, denied, unauthenticated, unavailable).", "model", "status")

// modelNameFromURI is "churn" for models:/churn/1 or models:/churn@champion.
func modelNameFromURI(uri string) (string, error) {
	rest, ok := strings.CutPrefix(uri, "models:/")
	if !ok {
		return "", fmt.Errorf("%q is not a models:/<name>/<version> URI", uri)
	}
	name := rest
	if i := strings.IndexAny(rest, "/@"); i >= 0 {
		name = rest[:i]
	}
	if name == "" {
		return "", fmt.Errorf("%q has no model name", uri)
	}
	return name, nil
}

// cmdServeModel is `duct mlflow serve-model <models:/name/version> [--port N] [--name n]`.
func cmdServeModel(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("mlflow serve-model", stderr)
	port := fs.Int("port", 0, "loopback port of the model server (default: [mlflow.serving.models.<name>] port)")
	name := fs.String("name", "", "name under /models/ (default: the model name of the URI)")
	workers := fs.Int("workers", 1, "gunicorn workers (each loads the model: memory x workers)")
	timeout := fs.Int("timeout", 60, "seconds a worker may take for one request")
	printOnly := fs.Bool("print", false, "print the command, do not start anything")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr("serve-model needs a model URI: duct mlflow serve-model models:/churn/1 --port 5001")
	}
	uri := pos[0]
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if p.Config.MLflow == nil {
		return errors.New("mlflow is not enabled: run `duct enable mlflow`")
	}
	if *name == "" {
		if *name, err = modelNameFromURI(uri); err != nil {
			return usageErr("%v", err)
		}
	}
	if !project.ValidModelName(*name) {
		return usageErr("model name %q: only letters, digits, . _ - are allowed", *name)
	}
	configured, err := p.MLflowServedModels()
	if err != nil {
		return err
	}
	if *port == 0 {
		*port = configured[*name].Port
	}
	if *port < 1 || *port > 65535 {
		return usageErr("no port: pass --port or set [mlflow.serving.models.%s] port", *name)
	}
	if *workers < 1 || *timeout < 1 {
		return usageErr("--workers and --timeout must be positive")
	}
	listen, err := p.MLflowListen()
	if err != nil {
		return err
	}
	argv := []string{"-m", "mlflow", "models", "serve", "-m", uri, "--host", "127.0.0.1", "--port", strconv.Itoa(*port),
		"--env-manager", "local", "--workers", strconv.Itoa(*workers), "--timeout", strconv.Itoa(*timeout)}
	if *printOnly {
		_, _ = fmt.Fprintf(stdout, "python %s\ntracking: http://%s\n", strings.Join(argv, " "), listen)
		return nil
	}
	if configured[*name].Port != *port {
		_, _ = fmt.Fprintf(stderr, "note: add to duct.toml so `duct serve` proxies it:\n[mlflow.serving.models.%s]\nport = %d\n", *name, *port)
	}
	py, err := preparePython(ctx, p, []pyNeed{{"mlflow", p.MLflowRequirement()}, {"duct", []string{p.PythonSDK()}}}, stderr)
	if err != nil {
		return err
	}
	// The model resolves through the tracking server (`duct mlflow`), which proxies the artifacts:
	// the model server gets no lake credentials, only the tracking URL.
	env := udf.SafeEnv(map[string]string{"MLFLOW_TRACKING_URI": "http://" + listen})
	// uvicorn (the scoring server) is found through PATH: put the interpreter's bin/ first.
	env = append(env, "PATH="+filepath.Dir(py[0])+string(os.PathListSeparator)+os.Getenv("PATH"))
	cmd := exec.CommandContext(ctx, py[0], append(append([]string{}, py[1:]...), argv...)...) //nolint:gosec // interpreter from duct.toml / the managed venv
	cmd.Dir = p.Root
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) } // graceful: gunicorn finishes in-flight requests
	cmd.WaitDelay = 20 * time.Second
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("mlflow models serve: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	// Readiness: /ping answers 200 once the model is loaded (the first start installs nothing,
	// but loading may download the artifacts). Then a periodic check reports a stuck server.
	ping := "http://127.0.0.1:" + strconv.Itoa(*port) + "/ping"
	healthy := func() bool {
		c := &http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get(ping)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}
	ready, deadline := false, time.After(10*time.Minute)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for !ready {
		select {
		case err := <-exited:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("mlflow models serve exited before it was ready: %v", err)
		case <-deadline:
			_ = cmd.Cancel()
			<-exited
			return fmt.Errorf("model %s did not become ready on 127.0.0.1:%d in 10 minutes", uri, *port)
		case <-ctx.Done():
			<-exited
			return nil
		case <-tick.C:
			ready = healthy()
		}
	}
	_, _ = fmt.Fprintf(stdout, "model %s ready on 127.0.0.1:%d (call it through `duct serve`: /models/%s/invocations)\n", uri, *port, *name)
	check := time.NewTicker(15 * time.Second)
	defer check.Stop()
	for fails := 0; ; {
		select {
		case err := <-exited:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("mlflow models serve: %v", err)
		case <-ctx.Done():
			<-exited // SIGINT to gunicorn, up to WaitDelay, then kill
			_, _ = fmt.Fprintf(stdout, "model %s stopped\n", *name)
			return nil
		case <-check.C:
			if healthy() {
				fails = 0
				continue
			}
			if fails++; fails == 3 {
				_, _ = fmt.Fprintf(stderr, "warning: model %s does not answer /ping (3 checks in a row)\n", *name)
			}
		}
	}
}

// modelProxy serves /models/<name>/invocations and /models/<name>/health.
type modelProxy struct {
	auth    contract.AuthProvider
	handler *governance.Handler
	models  map[string]project.MLflowServedModel
	roles   []string // [mlflow.serving] roles
	maxBody int64
	timeout time.Duration
	proxies map[string]*httputil.ReverseProxy
}

// mountModels mounts the proxy when [mlflow.serving.models] lists any model.
func mountModels(p *project.Project, m mounter, h *governance.Handler, auth contract.AuthProvider) {
	models, err := p.MLflowServedModels()
	if err != nil || len(models) == 0 {
		return
	}
	maxBody, timeout, err := p.MLflowServingLimits()
	if err != nil {
		return
	}
	mp := &modelProxy{auth: auth, handler: h, models: models, roles: p.Config.MLflow.Serving.Roles, maxBody: maxBody, timeout: timeout, proxies: map[string]*httputil.ReverseProxy{}}
	for name, mod := range models {
		target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(mod.Port))}
		rp := &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Header.Del("Authorization") // the backend never sees Duct credentials
			pr.Out.Header.Del("Cookie")
		}}
		rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, `{"error":"model server unavailable"}`, http.StatusBadGateway)
		}
		mp.proxies[name] = rp
	}
	m.Mount("/models/", mp)
}

func (mp *modelProxy) allowed(name string, id contract.Identity) bool {
	if governance.IsAdmin(id) {
		return true
	}
	roles := mp.roles
	if r := mp.models[name].Roles; len(r) > 0 {
		roles = r
	}
	return slices.Contains(roles, id.Role)
}

type codeWriter struct {
	http.ResponseWriter
	code int
}

func (c *codeWriter) WriteHeader(code int) { c.code = code; c.ResponseWriter.WriteHeader(code) }
func (c *codeWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, `{"error":`+strconv.Quote(msg)+"}\n")
}

func (mp *modelProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/models/"), "/"), "/")
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	name, action := parts[0], parts[1]
	backendPath, method := "", ""
	switch action {
	case "invocations":
		backendPath, method = "/invocations", http.MethodPost
	case "health":
		backendPath, method = "/ping", http.MethodGet
	default:
		http.NotFound(w, r)
		return
	}
	rp, ok := mp.proxies[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	rec := contract.AuditRecord{At: start.UTC(), Protocol: "models", User: "-", SQL: r.Method + " /models/" + name + "/" + action}
	finish := func(status int, errText string) {
		rec.Duration, rec.Err = time.Since(start), errText
		mp.handler.Audit(r.Context(), rec)
		label := "ok"
		switch {
		case status == http.StatusUnauthorized:
			label = "unauthenticated"
		case status == http.StatusForbidden:
			label = "denied"
		case status == http.StatusBadGateway || status == http.StatusGatewayTimeout:
			label = "unavailable"
		case status >= 400:
			label = "error"
		}
		modelRequests.Inc(name, label)
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		jsonErr(w, http.StatusMethodNotAllowed, "use "+method)
		finish(http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id, err := mp.authenticate(r)
	switch {
	case errors.Is(err, contract.ErrUnauthenticated):
		w.Header().Set("WWW-Authenticate", `Bearer realm="duct-models"`)
		jsonErr(w, http.StatusUnauthorized, "authentication required")
		finish(http.StatusUnauthorized, "unauthenticated")
		return
	case err != nil:
		jsonErr(w, http.StatusInternalServerError, "authentication unavailable")
		finish(http.StatusInternalServerError, "authentication unavailable")
		return
	}
	rec.User, rec.Role = id.User, id.Role
	if !mp.allowed(name, id) {
		jsonErr(w, http.StatusForbidden, "role "+id.Role+" may not call model "+name)
		finish(http.StatusForbidden, "permission denied: role "+id.Role+" may not call model "+name)
		return
	}
	if r.ContentLength > mp.maxBody {
		jsonErr(w, http.StatusRequestEntityTooLarge, "request body over the limit of "+strconv.FormatInt(mp.maxBody>>20, 10)+" MiB")
		finish(http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), mp.timeout)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, mp.maxBody)
	r.URL.Path, r.URL.RawPath = backendPath, ""
	cw := &codeWriter{ResponseWriter: w, code: http.StatusOK}
	rp.ServeHTTP(cw, r)
	errText := ""
	if cw.code >= 400 {
		errText = "model server answered " + strconv.Itoa(cw.code)
	}
	finish(cw.code, errText)
}

func (mp *modelProxy) authenticate(r *http.Request) (contract.Identity, error) {
	h := r.Header.Get("Authorization")
	switch {
	case strings.HasPrefix(h, "Bearer "):
		return mp.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "bearer", Secret: strings.TrimSpace(h[7:])})
	case strings.HasPrefix(h, "Basic "):
		if user, pass, ok := r.BasicAuth(); ok {
			return mp.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "password", User: user, Secret: pass})
		}
	}
	return contract.Identity{}, contract.ErrUnauthenticated
}
