package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // RFC 6455 handshake
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/notebooks"
	"github.com/alonsoburon/duct/core/project"
)

// ---- a stand-in for marimo ----

// fakeLauncher starts an HTTP server per "process" that answers like marimo
// would behind the console's proxy: it only talks to the password it was given,
// leaks a cookie and a CSP on purpose (the proxy must drop them) and does a
// websocket handshake that sends one text frame.
type fakeLauncher struct {
	mu    sync.Mutex
	procs []*fakeProc
}

type fakeProc struct {
	spec    notebooks.LaunchSpec
	srv     *httptest.Server
	done    chan struct{}
	stopped atomic.Bool
	mu      sync.Mutex
	seen    []http.Header // headers of every request that got through
}

func (p *fakeProc) Stop() {
	if p.stopped.CompareAndSwap(false, true) {
		p.srv.Close()
		close(p.done)
	}
}
func (p *fakeProc) Done() <-chan struct{} { return p.done }

func (l *fakeLauncher) Launch(_ context.Context, spec notebooks.LaunchSpec) (notebooks.Process, string, error) {
	p := &fakeProc{spec: spec, done: make(chan struct{})}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("duct:"+spec.Password))
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != want {
			http.Error(w, "marimo: missing token", http.StatusUnauthorized)
			return
		}
		p.mu.Lock()
		p.seen = append(p.seen, r.Header.Clone())
		p.mu.Unlock()
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			hj, _ := w.(http.Hijacker)
			c, rw, err := hj.Hijack()
			if err != nil {
				return
			}
			defer func() { _ = c.Close() }()
			sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")) //nolint:gosec // handshake
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
			msg := "hello-from-kernel"
			_, _ = rw.Write(append([]byte{0x81, byte(len(msg))}, msg...))
			_ = rw.Flush()
			time.Sleep(200 * time.Millisecond)
			return
		}
		w.Header().Set("Set-Cookie", "marimo_leak=1")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		_, _ = fmt.Fprintf(w, "kernel %s %s token=%t path=%s", spec.Mode, spec.Name, spec.Env["DUCT_TOKEN"] != "", r.URL.Path)
	}))
	l.mu.Lock()
	l.procs = append(l.procs, p)
	l.mu.Unlock()
	return p, p.srv.Listener.Addr().String(), nil
}

func (l *fakeLauncher) all() []*fakeProc {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*fakeProc{}, l.procs...)
}

// last is the newest process for (mode, name), or nil.
func (l *fakeLauncher) last(mode, name string) *fakeProc {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.procs) - 1; i >= 0; i-- {
		if p := l.procs[i]; p.spec.Mode == mode && p.spec.Name == name {
			return p
		}
	}
	return nil
}

// ---- harness ----

type nbServer struct {
	web, http string
	nb        string // the separate origin of the notebooks
	root      string
	launcher  *fakeLauncher
}

const nbToml = "\n[notebooks]\neditors = [\"duct_admin\", \"analistas\"]\nidle_timeout = \"%s\"\nmax_kernels = 6\n"

func startNotebookConsole(t *testing.T, idle string) *nbServer {
	t.Helper()
	l := &fakeLauncher{}
	s := startNotebookServer(t, idle, l)
	s.launcher = l
	return s
}

// startNotebookServer runs the server with launcher (nil: the real marimo).
func startNotebookServer(t *testing.T, idle string, launcher notebooks.Launcher) *nbServer {
	t.Helper()
	root := governedProject(t)
	mustDuct(t, "sql", "--project", root, "CREATE ROLE duct_admin")
	mustDuct(t, "grant", "select", "on", "gold.customers", "to", "duct_admin", "--project", root) // governance applies to the admin role too
	mustDuct(t, "user", "create", "jefa", "--role", "duct_admin", "--password", "pw-jefa-123", "--project", root)
	mustDuct(t, "user", "create", "bea", "--role", "analistas", "--password", "pw-bea-123", "--project", root)
	mustDuct(t, "user", "create", "otro", "--role", "vacio", "--password", "pw-otro-123", "--project", root)
	cfg := filepath.Join(root, "duct.toml")
	b, err := os.ReadFile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfg, string(b)+fmt.Sprintf(nbToml, idle))
	writeFile(t, filepath.Join(root, "notebooks", "hello.py"), "import marimo\n\napp = marimo.App()\n\n\n@app.cell\ndef _():\n    x = 1\n    return\n")

	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{
			Protocols: []string{"http", "web"}, Listen: map[string]string{"http": "127.0.0.1:0", "web": "127.0.0.1:0"},
			NoJobs: true, NoStream: true, NotebookLauncher: launcher,
			Ready: func(a map[string]string) { ready <- a },
		})
	}()
	var addrs map[string]string
	select {
	case addrs = <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("serve exited early: %v", err)
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("serve did not start")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("serve did not shut down")
		}
	})
	return &nbServer{web: addrs["web"], http: addrs["http"], nb: addrs["web-notebooks"], root: root}
}

func (s *nbServer) login(t *testing.T, user, pass string) *webConsole {
	t.Helper()
	w := &webConsole{t: t, base: "http://" + s.web}
	if !w.login(user, pass) {
		t.Fatalf("%s cannot log in", user)
	}
	return w
}

// cookie is the Cookie header of the console session.
func (w *webConsole) cookie() string {
	u, _ := url.Parse(w.base)
	var parts []string
	for _, c := range w.c.Jar.Cookies(u) {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}

// wsGet does a raw websocket upgrade against the console and returns the status
// line code and, on 101, the text of the first frame.
func wsGet(t *testing.T, addr, path string, hdr map[string]string) (int, string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var key [16]byte
	_, _ = rand.Read(key[:])
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n", path, addr, base64.StdEncoding.EncodeToString(key[:]))
	for k, v := range hdr {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("ws %s: %v", path, err)
	}
	var code int
	if _, err := fmt.Sscanf(line, "HTTP/1.1 %d", &code); err != nil {
		t.Fatalf("status line %q", line)
	}
	if code != 101 {
		return code, ""
	}
	for { // headers
		l, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if l == "\r\n" {
			break
		}
	}
	head := make([]byte, 2)
	if _, err := io.ReadFull(br, head); err != nil {
		t.Fatalf("ws frame: %v", err)
	}
	msg := make([]byte, head[1]&0x7f)
	if _, err := io.ReadFull(br, msg); err != nil {
		t.Fatalf("ws frame body: %v", err)
	}
	return code, string(msg)
}

func (w *webConsole) get(path string) (int, string, http.Header) {
	w.t.Helper()
	resp, b := w.do("GET", path, nil)
	return resp.StatusCode, b, resp.Header
}

// postHdr is a POST like Datastar's: CSRF in a header, JSON signals in the body.
func (w *webConsole) postSignals(path string, signals map[string]any) (int, string) {
	w.t.Helper()
	b, _ := json.Marshal(signals)
	req, _ := http.NewRequest("POST", w.base+path, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Datastar-Request", "true")
	req.Header.Set("X-CSRF-Token", w.csrf())
	resp, err := w.c.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

// nbBrowser is a browser on the notebook origin: its own cookie jar, entered with the
// ticket the console page puts in its iframe.
type nbBrowser struct {
	t    *testing.T
	base string
	c    *http.Client
}

var iframeSrc = regexp.MustCompile(`<iframe src="([^"]+)"`)

func newNBBrowser(t *testing.T, addr string) *nbBrowser {
	jar, _ := cookiejar.New(nil)
	return &nbBrowser{t: t, base: "http://" + addr, c: &http.Client{Jar: jar, Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

// ticketOf opens the console page of a notebook and returns the iframe address (the ticket URL) and the page.
func ticketOf(t *testing.T, w *webConsole, name, query string) (src, page string) {
	t.Helper()
	code, body, _ := w.get("/notebooks/" + name + query)
	if code != 200 {
		t.Fatalf("notebook page %s%s: %d %s", name, query, code, flashText(body))
	}
	m := iframeSrc.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no iframe in the page of %s: %s", name, textOnly(body))
	}
	return html.UnescapeString(m[1]), body
}

// enter opens a notebook as the console does and redeems the ticket in a fresh browser on the notebook origin.
func (s *nbServer) enter(t *testing.T, w *webConsole, name, query string) (*nbBrowser, string) {
	t.Helper()
	src, page := ticketOf(t, w, name, query)
	if !strings.HasPrefix(src, "http://"+s.nb+"/nb-auth?t=") {
		t.Fatalf("iframe src = %s, want the notebook origin %s", src, s.nb)
	}
	b := newNBBrowser(t, s.nb)
	resp, err := b.c.Get(src)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/nb/") {
		t.Fatalf("redeeming the ticket: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	return b, page
}

func (b *nbBrowser) get(path string) (int, string, http.Header) {
	b.t.Helper()
	resp, err := b.c.Get(b.base + path)
	if err != nil {
		b.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out), resp.Header
}

// cookie is the Cookie header the browser would send for path.
func (b *nbBrowser) cookie(path string) string {
	u, _ := url.Parse(b.base + path)
	var parts []string
	for _, c := range b.c.Jar.Cookies(u) {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}

func nbWait(t *testing.T, what string, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

var versionCount = regexp.MustCompile(`class="font-mono">#(\d+)</td>`)

// ---- tests ----

// TestNotebooksConsole drives the notebook pages of a real server with a fake
// marimo: roles, the per-file lock, the proxy, history, comments and lifecycle.
func TestNotebooksConsole(t *testing.T) {
	s := startNotebookConsole(t, "10m")
	ana := s.login(t, "ana", "pw-ana-123")
	bea := s.login(t, "bea", "pw-bea-123")
	jefa := s.login(t, "jefa", "pw-jefa-123")
	nadie := s.login(t, "nadie", "pw-nadie-123")
	var nviewer *nbBrowser // nadie on the notebook origin, in run mode (set by the first subtest that needs it)

	t.Run("list", func(t *testing.T) {
		code, body, _ := nadie.get("/notebooks")
		if code != 200 || !strings.Contains(body, "hello") || !strings.Contains(body, "solo ejecución") || strings.Contains(body, "Crear desde plantilla") {
			t.Errorf("viewer list: %d (create form must be hidden)", code)
		}
		code, body, _ = ana.get("/notebooks")
		if code != 200 || !strings.Contains(body, "Crear desde plantilla") || !strings.Contains(body, "Libre") {
			t.Errorf("editor list: %d", code)
		}
		if !strings.Contains(body, `aria-current="page"`) {
			t.Error("sidebar entry not active")
		}
	})

	t.Run("viewer cannot open edit mode", func(t *testing.T) {
		if code, _, _ := nadie.get("/notebooks/hello?mode=edit"); code != 403 {
			t.Errorf("viewer ?mode=edit: %d, want 403", code)
		}
		if p := s.launcher.last("edit", "hello"); p != nil {
			t.Fatal("an edit process started for a viewer")
		}
		// The viewer gets run mode, with its own process and its own token...
		vb, body := s.enter(t, nadie, "hello", "")
		if !strings.Contains(body, "Solo ejecución") || strings.Contains(body, "Historial") {
			t.Fatalf("viewer page: %s", textOnly(body))
		}
		nviewer = vb
		if code, body, _ := vb.get("/nb/run/hello/index"); code != 200 || !strings.Contains(body, "kernel run hello") {
			t.Errorf("viewer proxy: %d %q", code, body)
		}
		// ...and even asking the notebook origin directly there is nothing to reach in edit mode:
		// its cookie is for run mode only.
		if code, _, _ := vb.get("/nb/edit/hello/"); code != 401 {
			t.Errorf("viewer /nb/edit: %d, want 401", code)
		}
		// no history, no diff, no restore for viewers
		if code, _, _ := nadie.get("/notebooks/hello/history"); code != 403 {
			t.Errorf("viewer history: %d", code)
		}
		if resp, _ := nadie.post("/notebooks/hello/restore", url.Values{"version": {"1"}}); resp.StatusCode != 303 {
			t.Errorf("viewer restore: %d", resp.StatusCode)
		}
		if resp, _ := nadie.post("/notebooks", url.Values{"name": {"mio"}}); resp.StatusCode != 303 {
			t.Errorf("viewer create: %d", resp.StatusCode)
		}
		if _, err := os.Stat(filepath.Join(s.root, "notebooks", "mio.py")); err == nil {
			t.Error("a viewer created a notebook")
		}
	})

	t.Run("proxy auth", func(t *testing.T) {
		// No cookie: 401 for http, 403 before the upgrade for the websocket.
		resp, err := http.Get("http://" + s.nb + "/nb/run/hello/")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Errorf("http without cookie: %d", resp.StatusCode)
		}
		for _, path := range []string{"/nb/run/hello/ws?session_id=a", "/nb/edit/hello/ws", "/nb/run/hello/ws_sync"} {
			if c, _ := wsGet(t, s.nb, path, nil); c != 403 {
				t.Errorf("ws %s without cookie: %d, want 403", path, c)
			}
			if c, _ := wsGet(t, s.nb, path, map[string]string{"Cookie": "duct_nb=forged"}); c != 403 {
				t.Errorf("ws %s forged cookie: %d, want 403", path, c)
			}
			// the console's session cookie is not a credential of the notebook origin
			if c, _ := wsGet(t, s.nb, path, map[string]string{"Cookie": nadie.cookie()}); c != 403 {
				t.Errorf("ws %s with the console cookie: %d, want 403", path, c)
			}
		}
		// A real cookie still needs a same-origin upgrade.
		ck := nviewer.cookie("/nb/run/hello/ws")
		if ck == "" {
			t.Fatal("the notebook browser has no cookie")
		}
		if c, _ := wsGet(t, s.nb, "/nb/run/hello/ws", map[string]string{"Cookie": ck, "Origin": "http://evil.example"}); c != 403 {
			t.Errorf("cross-origin ws: %d, want 403", c)
		}
		// With the cookie it reaches the process (which only accepts marimo's password).
		c, frame := wsGet(t, s.nb, "/nb/run/hello/ws", map[string]string{"Cookie": ck, "Origin": "http://" + s.nb})
		if c != 101 || frame != "hello-from-kernel" {
			t.Errorf("authenticated ws: %d %q", c, frame)
		}
		// The cookie is for one notebook and one mode.
		if c, _ := wsGet(t, s.nb, "/nb/run/other/ws", map[string]string{"Cookie": ck, "Origin": "http://" + s.nb}); c != 403 {
			t.Errorf("ws of another notebook with this cookie: %d, want 403", c)
		}
		if c, _ := wsGet(t, s.nb, "/nb/edit/hello/ws", map[string]string{"Cookie": ck, "Origin": "http://" + s.nb}); c != 403 {
			t.Errorf("ws of the edit mode with a run cookie: %d, want 403", c)
		}
		// The browser's credentials and the process's headers do not cross.
		p := s.launcher.last("run", "hello")
		p.mu.Lock()
		for _, h := range p.seen {
			if h.Get("Cookie") != "" || strings.Contains(h.Get("Authorization"), "pw-") {
				t.Errorf("credentials leaked to the process: %v", h)
			}
		}
		p.mu.Unlock()
		_, _, hdr := nviewer.get("/nb/run/hello/x")
		csp := hdr.Get("Content-Security-Policy")
		if hdr.Get("Set-Cookie") != "" || strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "frame-ancestors http://"+s.web) {
			t.Errorf("response headers: %v", hdr)
		}
		// The Duct pages keep the strict policy.
		if _, _, h := nadie.get("/notebooks"); !strings.Contains(h.Get("Content-Security-Policy"), "frame-ancestors 'none'") && h.Get("X-Frame-Options") != "DENY" {
			t.Errorf("duct pages lost their policy: %v", h)
		}
	})

	t.Run("token scope is the user's role", func(t *testing.T) {
		query := func(tok string) string {
			req, _ := http.NewRequest("POST", "http://"+s.http+"/query", strings.NewReader("SELECT name, rut FROM customers ORDER BY name FORMAT CSV"))
			req.Header.Set("Authorization", "Bearer "+tok)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			b, _ := io.ReadAll(resp.Body)
			return resp.Status + " " + strings.TrimSpace(string(b))
		}
		// nadie (role vacio) has a token whose queries are denied: no escalation to the editors' data.
		v := s.launcher.last("run", "hello")
		if got := query(v.spec.Env["DUCT_TOKEN"]); !strings.HasPrefix(got, "4") && !strings.HasPrefix(got, "5") {
			t.Errorf("a role without grants queried customers through its notebook token: %s", got)
		}
		// ana edits: her token sees what analistas sees (row filter CL, rut masked).
		if code, _, _ := ana.get("/notebooks/hello"); code != 200 {
			t.Fatalf("ana opens: %d", code)
		}
		e := s.launcher.last("edit", "hello")
		if e == nil || e.spec.Env["DUCT_URL"] != "http://"+s.http {
			t.Fatalf("edit process = %+v", e)
		}
		for k := range e.spec.Env {
			if k != "DUCT_URL" && k != "DUCT_TOKEN" {
				t.Errorf("unexpected variable %s handed to the notebook process", k)
			}
		}
		got := query(e.spec.Env["DUCT_TOKEN"])
		if !strings.Contains(got, "Ana") || !strings.Contains(got, "***") || strings.Contains(got, "Beto") || strings.Contains(got, "11.111.111-1") {
			t.Errorf("ana's token does not carry her governance: %s", got)
		}
		// the token belongs to ana: it is listed under her name and labelled.
		rows := queryRows(t, s.root, "SELECT user_name, label FROM duct.duct_tokens WHERE label LIKE 'notebook %' AND NOT revoked ORDER BY id")
		if len(rows) < 2 {
			t.Errorf("notebook tokens = %v", rows)
		}
	})

	t.Run("lock per file", func(t *testing.T) {
		// ana holds the edit lock (opened above). bea gets run mode and a banner.
		bb, body := s.enter(t, bea, "hello", "")
		if !strings.Contains(body, "está editando") || !strings.Contains(body, "ana") || bb.cookie("/nb/run/hello/") == "" {
			t.Errorf("bea sees: lock banner or run mode missing")
		}
		bb, body = s.enter(t, bea, "hello", "?mode=edit")
		if !strings.Contains(body, "está editando") || bb.cookie("/nb/run/hello/") == "" || bb.cookie("/nb/edit/hello/") != "" {
			t.Errorf("bea ?mode=edit while locked must get run mode")
		}
		if code, _, _ := bb.get("/nb/edit/hello/"); code != 401 {
			t.Errorf("bea reaches ana's edit process: %d", code)
		}
		if _, body, _ := bea.get("/notebooks"); !strings.Contains(body, "Editando: ana") {
			t.Error("list does not show the lock")
		}
		// bea can't change the file under ana's feet.
		base := queryRows(t, s.root, "SELECT CAST(id AS VARCHAR) AS id FROM duct.duct_notebook_versions WHERE notebook = 'hello' ORDER BY id LIMIT 1")
		if len(base) != 1 {
			t.Fatal("opening in edit mode recorded no baseline version")
		}
		for _, act := range []struct {
			path string
			v    url.Values
		}{{"/notebooks/hello/rename", url.Values{"to": {"otro"}}}, {"/notebooks/hello/restore", url.Values{"version": {base[0]["id"].(string)}}}} {
			resp, _ := bea.post(act.path, act.v)
			if b := bea.follow(resp); !strings.Contains(b, "being edited by someone else") {
				t.Errorf("%s while locked: %s", act.path, flashText(b))
			}
		}
		if resp, _ := jefa.post("/notebooks/hello/delete", nil); resp.StatusCode != 303 {
			t.Errorf("admin delete: %d", resp.StatusCode)
		} else if _, err := os.Stat(filepath.Join(s.root, "notebooks", "hello.py")); err != nil {
			t.Error("a locked notebook was deleted")
		}
		// ana herself reuses her process (no second one).
		n := len(s.launcher.all())
		if code, _, _ := ana.get("/notebooks/hello"); code != 200 || len(s.launcher.all()) != n {
			t.Errorf("ana's second open started another process (%d -> %d)", n, len(s.launcher.all()))
		}
	})

	t.Run("history", func(t *testing.T) {
		path := filepath.Join(s.root, "notebooks", "hello.py")
		orig, _ := os.ReadFile(path)
		// marimo saves: the janitor records each version for the user holding the lock.
		saves := []string{string(orig) + "    y = 2\n", string(orig) + "    y = 2\n    z = 3\n"}
		for i, c := range saves {
			if err := os.WriteFile(path, []byte(c), 0o600); err != nil {
				t.Fatal(err)
			}
			want := i + 2 // baseline + saves so far
			nbWait(t, "version recorded", 20*time.Second, func() bool {
				rows := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_notebook_versions WHERE notebook = 'hello'")
				return int(rows[0]["n"].(float64)) >= want
			})
		}
		_, body, _ := ana.get("/notebooks/hello/history")
		if !versionCount.MatchString(body) || !strings.Contains(body, "Versión inicial") || !strings.Contains(body, "Guardado") || !strings.Contains(body, ">ana<") {
			t.Fatalf("history page: %s", textOnly(body))
		}
		rows := queryRows(t, s.root, "SELECT CAST(id AS VARCHAR) AS id, author, action, lines_added, lines_removed, length(content_hash) AS h FROM duct.duct_notebook_versions WHERE notebook = 'hello' ORDER BY id")
		if len(rows) != 3 || rows[0]["action"] != "baseline" || rows[1]["author"] != "ana" || rows[1]["lines_added"] != float64(1) || rows[2]["lines_added"] != float64(1) || rows[1]["h"] != float64(64) {
			t.Fatalf("versions = %v", rows)
		}
		second := rows[1]["id"].(string)
		_, body, _ = ana.get("/notebooks/hello/history?to=" + second)
		if !strings.Contains(body, "y = 2") || !strings.Contains(body, "text-success") {
			t.Errorf("diff does not show the added line: %s", textOnly(body))
		}
		_, body, _ = ana.get("/notebooks/hello/history?from=" + second) // against the current file
		if !strings.Contains(body, "z = 3") {
			t.Errorf("diff against current: %s", textOnly(body))
		}
		// Restore needs the lock: ana owns it, so her own process is stopped and the file goes back.
		first := rows[0]["id"].(string)
		resp, _ := ana.post("/notebooks/hello/restore", url.Values{"version": {first}})
		if b := ana.follow(resp); !strings.Contains(b, "Versión restaurada") {
			t.Fatalf("restore: %s", flashText(b))
		}
		now, _ := os.ReadFile(path)
		if string(now) != string(orig) {
			t.Errorf("file after restore = %q", now)
		}
		rows = queryRows(t, s.root, "SELECT action, author FROM duct.duct_notebook_versions WHERE notebook = 'hello' ORDER BY id DESC LIMIT 1")
		if rows[0]["action"] != "restore" || rows[0]["author"] != "ana" {
			t.Errorf("restore not recorded: %v", rows)
		}
		if got := s.launcher.last("edit", "hello"); !got.stopped.Load() {
			t.Error("restoring left the stale editor running")
		}
		// the lock is free again
		if _, body, _ := bea.get("/notebooks"); !strings.Contains(body, "Libre") {
			t.Error("lock not released after restore")
		}
	})

	t.Run("kernel dies on logout", func(t *testing.T) {
		if code, _, _ := ana.get("/notebooks/hello"); code != 200 {
			t.Fatal("ana reopens")
		}
		e := s.launcher.last("edit", "hello")
		if e.stopped.Load() {
			t.Fatal("fresh process already stopped")
		}
		tok := e.spec.Env["DUCT_TOKEN"]
		if resp, _ := ana.post("/logout", nil); resp.StatusCode != 303 {
			t.Fatalf("logout: %d", resp.StatusCode)
		}
		nbWait(t, "process stopped by logout", 10*time.Second, func() bool { return e.stopped.Load() })
		rows := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_tokens WHERE NOT revoked AND label LIKE 'notebook hello (edit)'")
		if rows[0]["n"] != float64(0) {
			t.Errorf("the process token was not revoked: %v", rows)
		}
		req, _ := http.NewRequest("POST", "http://"+s.http+"/query", strings.NewReader("SELECT 1"))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Errorf("revoked notebook token still works: %d", resp.StatusCode)
		}
		ana = s.login(t, "ana", "pw-ana-123")
	})

	t.Run("comments", func(t *testing.T) {
		code, body := nadie.postSignals("/notebooks/hello/comments", map[string]any{"cref": "celda 1", "cbody": "¿Por qué <b>x</b>?"})
		if code != 200 || !strings.Contains(body, "nb-comments") || !strings.Contains(body, "&lt;b&gt;x&lt;/b&gt;") || !strings.Contains(body, "celda 1") {
			t.Fatalf("add comment: %d %s", code, body)
		}
		if code, body := nadie.postSignals("/notebooks/hello/comments", map[string]any{"cbody": "   "}); code != 200 || !strings.Contains(body, "the comment is empty") {
			t.Errorf("empty comment: %d %s", code, body)
		}
		rows := queryRows(t, s.root, "SELECT CAST(id AS VARCHAR) AS id, author, resolved FROM duct.duct_notebook_comments WHERE notebook = 'hello'")
		if len(rows) != 1 || rows[0]["author"] != "nadie" || rows[0]["resolved"] != float64(0) {
			t.Fatalf("comments = %v", rows)
		}
		id := rows[0]["id"].(string)
		// shown in the page of the notebook, panel around the embedded process
		if _, page, _ := bea.get("/notebooks/hello"); !strings.Contains(page, "celda 1") || !strings.Contains(page, "1 abiertos") {
			t.Errorf("comments panel missing from the page")
		}
		// another viewer (not the author, not an editor) cannot resolve it; an editor can; the author can reopen
		otro := s.login(t, "otro", "pw-otro-123")
		if code, body := otro.postSignals("/notebooks/hello/comments/"+id+"/resolve", nil); code != 200 || !strings.Contains(body, "only the author or an editor") {
			t.Errorf("stranger resolve: %d %s", code, textOnly(body))
		}
		if rows := queryRows(t, s.root, "SELECT resolved FROM duct.duct_notebook_comments"); rows[0]["resolved"] != float64(0) {
			t.Error("a stranger resolved the comment")
		}
		code, body = bea.postSignals("/notebooks/hello/comments/"+id+"/resolve", nil)
		if code != 200 || !strings.Contains(body, "Resuelto por bea") {
			t.Errorf("editor resolve: %d %s", code, textOnly(body))
		}
		if code, body := nadie.postSignals("/notebooks/hello/comments/"+id+"/reopen", nil); code != 200 || strings.Contains(body, "Resuelto por") {
			t.Errorf("author reopen: %d", code)
		}
		if resp, _ := nadie.post("/notebooks/hello/comments/"+id+"/delete", nil); resp.StatusCode == 200 {
			t.Error("unknown comment action accepted")
		}
	})

	t.Run("create rename delete", func(t *testing.T) {
		resp, _ := ana.post("/notebooks", url.Values{"name": {"ventas"}})
		if resp.StatusCode != 303 || !strings.HasPrefix(resp.Header.Get("Location"), "/notebooks/ventas") {
			t.Fatalf("create: %d %s", resp.StatusCode, resp.Header.Get("Location"))
		}
		b, err := os.ReadFile(filepath.Join(s.root, "notebooks", "ventas.py"))
		if err != nil || !strings.Contains(string(b), "duct.connect()") || !strings.Contains(string(b), "marimo.App") {
			t.Fatalf("template: %v %q", err, b)
		}
		// bad and duplicate names
		for _, bad := range []string{"../x", "a b", "", ".oculto", "ventas"} {
			resp, _ := ana.post("/notebooks", url.Values{"name": {bad}})
			if b := ana.follow(resp); !strings.Contains(b, `role="alert"`) {
				t.Errorf("name %q accepted", bad)
			}
		}
		if _, err := os.Stat(filepath.Join(s.root, "..", "x.py")); err == nil {
			t.Error("path traversal")
		}
		// a comment and a version travel with a rename
		if code, _ := ana.postSignals("/notebooks/ventas/comments", map[string]any{"cbody": "revisar totales"}); code != 200 {
			t.Fatal("comment")
		}
		resp, _ = ana.post("/notebooks/ventas/rename", url.Values{"to": {"ventas_mensuales"}})
		if resp.StatusCode != 303 || !strings.HasPrefix(resp.Header.Get("Location"), "/notebooks/ventas_mensuales") {
			t.Fatalf("rename: %d %s", resp.StatusCode, resp.Header.Get("Location"))
		}
		if _, err := os.Stat(filepath.Join(s.root, "notebooks", "ventas.py")); err == nil {
			t.Error("old file still there")
		}
		rows := queryRows(t, s.root, "SELECT (SELECT count(*) FROM duct.duct_notebook_comments WHERE notebook = 'ventas_mensuales') AS c, (SELECT count(*) FROM duct.duct_notebook_versions WHERE notebook = 'ventas_mensuales') AS v, (SELECT count(*) FROM duct.duct_notebook_versions WHERE notebook = 'ventas') AS old")
		if rows[0]["c"] != float64(1) || rows[0]["v"].(float64) < 2 || rows[0]["old"] != float64(0) {
			t.Errorf("history/comments did not move: %v", rows)
		}
		// the creator is recorded
		if _, body, _ := ana.get("/notebooks"); !strings.Contains(body, "ventas_mensuales") || !strings.Contains(body, ">ana<") {
			t.Errorf("list lacks the new notebook or its creator")
		}
		// delete: editors who are not admins are refused; the admin deletes, history stays and a restore brings it back
		if resp, _ := ana.post("/notebooks/ventas_mensuales/delete", nil); resp.StatusCode != 303 {
			t.Fatal("delete by editor")
		}
		if _, err := os.Stat(filepath.Join(s.root, "notebooks", "ventas_mensuales.py")); err != nil {
			t.Error("a non-admin deleted a notebook")
		}
		if _, body, _ := ana.get("/notebooks/ventas_mensuales?mode=run"); strings.Contains(body, "Confirmar eliminación") {
			t.Error("delete offered to a non-admin")
		}
		if _, body, _ := jefa.get("/notebooks/ventas_mensuales?mode=run"); !strings.Contains(body, "Confirmar eliminación") {
			t.Error("delete not offered to the admin")
		}
		resp, _ = jefa.post("/notebooks/ventas_mensuales/delete", nil)
		if b := jefa.follow(resp); !strings.Contains(b, "eliminado") {
			t.Fatalf("admin delete: %s", flashText(b))
		}
		if _, err := os.Stat(filepath.Join(s.root, "notebooks", "ventas_mensuales.py")); err == nil {
			t.Error("file not deleted")
		}
		if code, _, _ := ana.get("/notebooks/ventas_mensuales"); code != 404 {
			t.Errorf("deleted notebook page: %d", code)
		}
		_, hist, _ := ana.get("/notebooks/ventas_mensuales/history")
		if !strings.Contains(hist, "Eliminado") {
			t.Fatalf("history of a deleted notebook: %s", textOnly(hist))
		}
		last := queryRows(t, s.root, "SELECT CAST(id AS VARCHAR) AS id FROM duct.duct_notebook_versions WHERE notebook = 'ventas_mensuales' AND action = 'create'")
		resp, _ = ana.post("/notebooks/ventas_mensuales/restore", url.Values{"version": {last[0]["id"].(string)}})
		if b := ana.follow(resp); !strings.Contains(b, "Versión restaurada") {
			t.Fatalf("restore after delete: %s", flashText(b))
		}
		if _, err := os.Stat(filepath.Join(s.root, "notebooks", "ventas_mensuales.py")); err != nil {
			t.Error("restore did not recreate the file")
		}
	})

	t.Run("audit", func(t *testing.T) {
		_, aud, _ := jefa.get("/admin/audit?protocol=web")
		for _, want := range []string{"NOTEBOOK open mode=run name=hello", "NOTEBOOK open mode=edit name=hello", "NOTEBOOK create name=ventas",
			"NOTEBOOK rename name=ventas to=ventas_mensuales", "NOTEBOOK delete name=ventas_mensuales", "NOTEBOOK restore name=hello", "NOTEBOOK comment name=hello",
			"NOTEBOOK resolve comment name=hello", "NOTEBOOK diff name=hello"} {
			if !strings.Contains(aud, want) {
				t.Errorf("audit lacks %q", want)
			}
		}
		_, den, _ := jefa.get("/admin/audit?protocol=web&status=denied")
		for _, want := range []string{"NOTEBOOK open mode=edit name=hello", "NOTEBOOK create name=mio", "NOTEBOOK delete name=ventas_mensuales"} {
			if !strings.Contains(den, want) {
				t.Errorf("denied audit lacks %q", want)
			}
		}
		if strings.Contains(aud, "revisar totales") || strings.Contains(aud, "Por qué") {
			t.Error("comment text reached the audit log")
		}
	})
}

func flashText(page string) string {
	m := regexp.MustCompile(`(?s)role="(?:alert|status)".*?</p>`).FindString(page)
	return textOnly(m)
}

func textOnly(html string) string {
	return strings.Join(strings.Fields(regexp.MustCompile(`<[^>]*>`).ReplaceAllString(html, " ")), " ")
}

// TestNotebooksIdle: a process lives while its user is active, dies after idle_timeout
// (token revoked, lock released, last save recorded) and the proxy then has nothing to reach.
func TestNotebooksIdle(t *testing.T) {
	s := startNotebookConsole(t, "2s")
	bea := s.login(t, "bea", "pw-bea-123")
	ana := s.login(t, "ana", "pw-ana-123")
	bb, _ := s.enter(t, bea, "hello", "")
	e := s.launcher.last("edit", "hello")
	if e == nil || e.stopped.Load() {
		t.Fatal("bea has no edit process")
	}
	for range 4 { // activity keeps it alive well past the 2 s timeout
		time.Sleep(time.Second)
		if code, _, _ := bb.get("/nb/edit/hello/ping"); code != 200 {
			t.Fatalf("proxy while active: %d", code)
		}
	}
	if e.stopped.Load() {
		t.Fatal("an active process was killed")
	}
	// bea saves, then goes quiet: the idle kill still records her last save.
	path := filepath.Join(s.root, "notebooks", "hello.py")
	b, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(b, "    # last words\n"...), 0o600); err != nil {
		t.Fatal(err)
	}
	nbWait(t, "idle kill", 15*time.Second, func() bool { return e.stopped.Load() })
	if code, _, _ := bb.get("/nb/edit/hello/ping"); code != 404 {
		t.Errorf("proxy to a dead process: %d", code)
	}
	rows := queryRows(t, s.root, "SELECT author FROM duct.duct_notebook_versions WHERE notebook = 'hello' AND action = 'save'")
	if len(rows) == 0 || rows[len(rows)-1]["author"] != "bea" {
		t.Errorf("last save not recorded for bea: %v", rows)
	}
	if n := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_tokens WHERE label LIKE 'notebook %' AND NOT revoked")[0]["n"]; n != float64(0) {
		t.Errorf("tokens left after the kill: %v", n)
	}
	// the lock is free: ana edits now
	resp, body := ana.do("GET", "/notebooks/hello", nil)
	if resp.StatusCode != 200 || strings.Contains(body, "Solo lectura") {
		t.Errorf("ana after bea's idle kill: %d %s", resp.StatusCode, textOnly(body))
	}
	if last := s.launcher.last("edit", "hello"); last == e {
		t.Error("ana got bea's dead process")
	}
}

// wsHold upgrades like wsGet but keeps the connection open for d (marimo's run mode
// executes the notebook while its websocket session lives) and drains what arrives.
func wsHold(t *testing.T, addr, path string, hdr map[string]string, d time.Duration) int {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var key [16]byte
	_, _ = rand.Read(key[:])
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n", path, addr, base64.StdEncoding.EncodeToString(key[:]))
	for k, v := range hdr {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	if _, err := c.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(d))
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("ws %s: %v", path, err)
	}
	var code int
	_, _ = fmt.Sscanf(line, "HTTP/1.1 %d", &code)
	_, _ = io.Copy(io.Discard, br) // until the deadline
	return code
}

// procsWith counts processes whose command line contains s (Linux; -1 elsewhere).
func procsWith(s string) int {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err == nil && strings.Contains(strings.ReplaceAll(string(b), "\x00", " "), s) {
			n++
		}
	}
	return n
}

// marimoTestNotebook runs a governed query and probes its surroundings, then POSTs what it found to
// the test's own listener on loopback (the sandbox shares the network, and has no other way to write
// to the host). __ROOT__ and __REPORT__ are replaced by the test.
const marimoTestNotebook = `import marimo

app = marimo.App()


@app.cell
def _():
    import json
    import os
    import urllib.request

    import duct

    root = "__ROOT__"
    with duct.connect() as con:
        res = con.sql("SELECT name, rut FROM customers ORDER BY name")

    def tryit(f):
        try:
            return f()
        except Exception as e:
            return "ERR " + type(e).__name__

    def write(path):
        with open(path, "a") as f:
            f.write("x")
        return "ok"

    nbdir = os.path.join(root, "notebooks")
    report = {
        "rows": [(r["name"], r["rut"]) for r in res.rows],
        "env": sorted(k for k in os.environ if k.startswith(("DUCT_", "AWS_", "PG"))),
        "root": tryit(lambda: sorted(os.listdir(root))),
        "duct_dir": tryit(lambda: os.listdir(os.path.join(root, ".duct"))),
        "lake": tryit(lambda: os.listdir(os.path.join(root, "lake"))),
        "config": tryit(lambda: open(os.path.join(root, "duct.toml")).read()[:20]),
        "nb_write": tryit(lambda: write(os.path.join(nbdir, "intruso.txt"))),
        "tmp_write": tryit(lambda: write("/tmp/probe")),
        "procs": len([p for p in os.listdir("/proc") if p.isdigit()]),
    }
    urllib.request.urlopen(urllib.request.Request("__REPORT__", data=json.dumps(report).encode()), timeout=10).read()
    return


if __name__ == "__main__":
    app.run()
`

// TestNotebooksMarimo runs the real marimo behind the console, sandboxed (bubblewrap) when
// it is available: edit and run mode per user, the proxy for http and websocket on the notebook
// origin, governed queries with the user's own token, the sandbox (no lake, no .duct/, the
// notebooks dir read-only in run mode and writable in edit mode), the lock, and the process
// ending at logout. Needs python/.venv (marimo).
func TestNotebooksMarimo(t *testing.T) {
	pythonEnv(t)
	t.Setenv("DUCT_S3_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("PGPASSWORD", "super-secret")
	sandboxed := false
	if _, err := notebooks.BwrapAvailable(); err == nil {
		sandboxed = true
	} else {
		t.Logf("bubblewrap not usable (%v): running the notebooks unsandboxed", err)
	}
	s := startNotebookServer(t, "10m", nil)
	reports := make(chan map[string]any, 8)
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]any
		if json.NewDecoder(r.Body).Decode(&m) == nil {
			reports <- m
		}
	}))
	defer rl.Close()
	src := strings.NewReplacer("__ROOT__", s.root, "__REPORT__", rl.URL).Replace(marimoTestNotebook)
	writeFile(t, filepath.Join(s.root, "notebooks", "clientes.py"), src)
	// the lake and .duct/ hold something worth stealing (a real project has both)
	writeFile(t, filepath.Join(s.root, "lake", "secreto.txt"), "datos del lago")
	next := func(what string) map[string]any {
		t.Helper()
		select {
		case m := <-reports:
			return m
		case <-time.After(60 * time.Second):
			t.Fatalf("no report from %s", what)
			return nil
		}
	}
	ana := s.login(t, "ana", "pw-ana-123")
	jefa := s.login(t, "jefa", "pw-jefa-123")
	nadie := s.login(t, "nadie", "pw-nadie-123")

	// Run mode: ana's process runs the notebook as analistas; jefa's as the admin.
	anaRun, _ := s.enter(t, ana, "clientes", "?mode=run")
	if code, body, hdr := anaRun.get("/nb/run/clientes/"); code != 200 || !strings.Contains(body, "marimo") || !strings.Contains(hdr.Get("Content-Security-Policy"), "frame-ancestors http://"+s.web) {
		t.Fatalf("marimo through the proxy: %d %.200s", code, body)
	}
	if code := wsHold(t, s.nb, "/nb/run/clientes/ws?session_id=ana1", map[string]string{"Cookie": anaRun.cookie("/nb/run/clientes/ws"), "Origin": "http://" + s.nb}, 6*time.Second); code != 101 {
		t.Fatalf("ana ws: %d", code)
	}
	got := next("ana's run")
	if rows := fmt.Sprint(got["rows"]); !strings.Contains(rows, "Ana ***") || !strings.Contains(rows, "Cami ***") || strings.Contains(rows, "Beto") || fmt.Sprint(got["env"]) != "[DUCT_TOKEN DUCT_URL]" {
		t.Errorf("ana's run = %v", got)
	}
	if sandboxed {
		// The sandbox: only the notebooks dir exists, read-only in run mode; /tmp is private; /proc is its own.
		if fmt.Sprint(got["root"]) != "[notebooks]" || !strings.HasPrefix(fmt.Sprint(got["duct_dir"]), "ERR ") || !strings.HasPrefix(fmt.Sprint(got["lake"]), "ERR ") ||
			!strings.HasPrefix(fmt.Sprint(got["config"]), "ERR ") {
			t.Errorf("the sandbox exposes the project: root=%v .duct=%v lake=%v duct.toml=%v", got["root"], got["duct_dir"], got["lake"], got["config"])
		}
		if !strings.HasPrefix(fmt.Sprint(got["nb_write"]), "ERR ") || got["tmp_write"] != "ok" {
			t.Errorf("run mode: notebooks dir write=%v (want an error), private tmp write=%v (want ok)", got["nb_write"], got["tmp_write"])
		}
		if n, _ := got["procs"].(float64); n > 12 {
			t.Errorf("the sandbox sees %v processes of the host", got["procs"])
		}
	} else if fmt.Sprint(got["root"]) == "[notebooks]" {
		t.Errorf("unsandboxed notebook sees a sandboxed project: %v", got["root"])
	}
	if _, err := os.Stat(filepath.Join(s.root, "notebooks", "intruso.txt")); err == nil && sandboxed {
		t.Error("a run-mode notebook wrote into the notebooks dir")
	}
	jefaRun, _ := s.enter(t, jefa, "clientes", "?mode=run")
	if code := wsHold(t, s.nb, "/nb/run/clientes/ws?session_id=jefa1", map[string]string{"Cookie": jefaRun.cookie("/nb/run/clientes/ws"), "Origin": "http://" + s.nb}, 6*time.Second); code != 101 {
		t.Fatalf("jefa ws: %d", code)
	}
	if rows := fmt.Sprint(next("jefa's run")["rows"]); !strings.Contains(rows, "Beto") || !strings.Contains(rows, "11.111.111-1") {
		t.Errorf("the admin's run should see everything: %s", rows)
	}
	// A viewer cannot reach edit mode, with marimo or without it.
	if code, _, _ := nadie.get("/notebooks/clientes?mode=edit"); code != 403 {
		t.Errorf("viewer edit page: %d", code)
	}
	nadieRun, _ := s.enter(t, nadie, "clientes", "?mode=run")
	if code, _, _ := nadieRun.get("/nb/edit/clientes/"); code != 401 {
		t.Errorf("viewer edit proxy: %d", code)
	}
	if c, _ := wsGet(t, s.nb, "/nb/edit/clientes/ws?session_id=x", map[string]string{"Cookie": nadieRun.cookie("/nb/run/clientes/ws"), "Origin": "http://" + s.nb}); c != 403 {
		t.Errorf("viewer edit websocket: %d", c)
	}

	// Edit mode: ana edits (real marimo edit), jefa is told the file is locked.
	anaEdit, _ := s.enter(t, ana, "clientes", "")
	if code, body, _ := anaEdit.get("/nb/edit/clientes/"); code != 200 || !strings.Contains(body, "marimo") {
		t.Fatalf("edit mode through the proxy: %d", code)
	}
	if c := wsHold(t, s.nb, "/nb/edit/clientes/ws?session_id=ed1", map[string]string{"Cookie": anaEdit.cookie("/nb/edit/clientes/ws"), "Origin": "http://" + s.nb}, 8*time.Second); c != 101 {
		t.Errorf("edit websocket: %d", c)
	}
	select { // marimo edit runs the notebook when it opens: its notebooks dir is writable
	case m := <-reports:
		if sandboxed && (m["nb_write"] != "ok" || fmt.Sprint(m["root"]) != "[notebooks]") {
			t.Errorf("edit mode in the sandbox: notebooks dir write=%v root=%v", m["nb_write"], m["root"])
		}
	case <-time.After(20 * time.Second):
		t.Log("marimo edit did not run the notebook on open: edit-mode probes skipped")
	}
	if _, body := s.enter(t, jefa, "clientes", ""); !strings.Contains(body, "está editando") {
		t.Errorf("jefa should get the lock banner and run mode")
	}
	if n := procsWith("--base-url /nb/edit/clientes"); n == 0 {
		t.Error("no marimo edit process is running")
	}
	// the process line holds no password: it comes through stdin
	if n := procsWith("--token-password "); n != 0 {
		t.Errorf("marimo's password is on a command line (%d)", n)
	}
	// Logout ends every process of the user (the kernel too), within the grace period, and
	// the notebook cookie she holds stops working at once.
	if resp, _ := ana.post("/logout", nil); resp.StatusCode != 303 {
		t.Fatalf("logout: %d", resp.StatusCode)
	}
	if code, _, _ := anaEdit.get("/nb/edit/clientes/"); code != 401 {
		t.Errorf("the notebook cookie survived the console logout: %d", code)
	}
	nbWait(t, "marimo processes gone after logout", 20*time.Second, func() bool { return procsWith("--base-url /nb/edit/clientes") == 0 })
}

// TestNotebooksOrigin: the notebooks live on their own origin, entered with a one-time ticket;
// the console serves none of /nb, the console's cookie is worth nothing on the notebook origin and
// the notebook cookie dies with the console session.
func TestNotebooksOrigin(t *testing.T) {
	s := startNotebookConsole(t, "10m")
	ana := s.login(t, "ana", "pw-ana-123")
	if s.nb == "" || s.nb == s.web {
		t.Fatalf("notebook origin = %q (console %q)", s.nb, s.web)
	}
	// the console has no notebook proxy any more, session or not
	for _, p := range []string{"/nb/run/hello/", "/nb/edit/hello/ws"} {
		if code, _, _ := ana.get(p); code != 404 {
			t.Errorf("console %s: %d, want 404", p, code)
		}
	}
	src, page := ticketOf(t, ana, "hello", "")
	_, _, hdr := ana.get("/notebooks/hello")
	if csp := hdr.Get("Content-Security-Policy"); !strings.Contains(csp, "frame-src http://"+s.nb+";") || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Errorf("the notebook page's CSP: %s", csp)
	}
	if !strings.Contains(page, "Notebook hello") {
		t.Error("iframe missing")
	}
	// the ticket opens once
	b1 := newNBBrowser(t, s.nb)
	for i, want := range []int{303, 403} {
		resp, err := b1.c.Get(src)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("redeem #%d: %d, want %d", i+1, resp.StatusCode, want)
		}
	}
	if code, body, _ := b1.get("/nb/edit/hello/index"); code != 200 || !strings.Contains(body, "kernel edit hello") {
		t.Errorf("notebook through its origin: %d %q", code, body)
	}
	// the console's cookie is not a credential on the notebook origin, and the notebook origin has no console
	anon := newNBBrowser(t, s.nb)
	req, _ := http.NewRequest("GET", anon.base+"/nb/edit/hello/index", http.NoBody)
	req.Header.Set("Cookie", ana.cookie())
	resp, err := anon.c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("console cookie on the notebook origin: %d", resp.StatusCode)
	}
	for _, p := range []string{"/catalog", "/notebooks", "/sql/run", "/logout"} {
		if code, _, _ := b1.get(p); code != 404 {
			t.Errorf("notebook origin %s: %d, want 404", p, code)
		}
	}
	// cross-site and cross-origin requests to the notebook origin are refused even with the cookie
	req, _ = http.NewRequest("GET", b1.base+"/nb/edit/hello/index", http.NoBody)
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	if resp, err = b1.c.Do(req); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Errorf("cross-site request: %d", resp.StatusCode)
	}
	// logging out of the console ends the notebook session at once
	if resp, _ := ana.post("/logout", nil); resp.StatusCode != 303 {
		t.Fatalf("logout: %d", resp.StatusCode)
	}
	if code, _, _ := b1.get("/nb/edit/hello/index"); code != 401 {
		t.Errorf("notebook cookie after the console logout: %d, want 401", code)
	}
	if c, _ := wsGet(t, s.nb, "/nb/edit/hello/ws", map[string]string{"Cookie": b1.cookie("/nb/edit/hello/ws"), "Origin": "http://" + s.nb}); c != 403 {
		t.Errorf("websocket after the console logout: %d, want 403", c)
	}
}
