package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/project"
)

const testNotebook = `import marimo

app = marimo.App()


@app.cell
def _():
    import os

    import marimo as mo

    import duct

    args = mo.cli_args()
    with duct.connect() as con:
        res = con.sql("SELECT name, rut, amt FROM customers ORDER BY amt")
    print("ROWS", [(r["name"], r["rut"], r["amt"]) for r in res.rows])
    print("ARROW", res.to_arrow().num_rows, type(con).__name__)
    print("X", args.get("x"))
    print("ENV", sorted(k for k in os.environ if k.startswith(("DUCT_", "AWS_", "PG"))))
    return


if __name__ == "__main__":
    app.run()
`

// TestNotebookScript: `duct notebook --script` runs a marimo notebook as the user, through
// a private duct serve: governed rows, arguments without --, credentials only as DUCT_URL/TOKEN.
func TestNotebookScript(t *testing.T) {
	pythonEnv(t)
	t.Setenv("DUCT_S3_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("DUCT_CATALOG_PASSWORD", "super-secret")
	t.Setenv("PGPASSWORD", "super-secret")
	t.Setenv("DUCT_URL", "") // not a running server: the command starts its own
	t.Setenv("DUCT_TOKEN", "")
	root := governedProject(t)
	writeFile(t, filepath.Join(root, "notebooks", "nb.py"), testNotebook)

	out, errs, code := duct(t, "notebook", "--script", "--user", "ana", "--project", root, "notebooks/nb.py", "--", "--x", "1")
	if code != 0 {
		t.Fatalf("code=%d out=%q err=%q", code, out, errs)
	}
	for _, want := range []string{
		"ROWS [('Ana', '***', 100), ('Cami', '***', 300)]", // row filter and mask of the role
		"ARROW 2 HTTPConnection",
		"X 1",
		"ENV ['DUCT_TOKEN', 'DUCT_URL']", // no lake or catalog credentials
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s\n%s", want, out, errs)
		}
	}
	// The token minted for the session is revoked when it ends.
	rows := queryRows(t, root, "SELECT count(*) AS n FROM duct.duct_tokens WHERE label = 'duct notebook' AND NOT revoked")
	if rows[0]["n"] != float64(0) {
		t.Errorf("session token not revoked: %v", rows)
	}
	// Without a way to reach the lake it explains what to do.
	if _, errs, code := duct(t, "notebook", "--script", "--project", root, "notebooks/nb.py"); code != 2 || !strings.Contains(errs, "--user") {
		t.Errorf("no user: code=%d err=%q", code, errs)
	}
}

// startNotebookServe runs duct serve with the notebook protocol and returns its addresses.
func startNotebookServe(t *testing.T, root string) map[string]string {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	p.Config.Serve.Modules = map[string]map[string]any{"notebook": {"listen": "127.0.0.1:0", "role": "analistas"}}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{
			Protocols: []string{"http", "notebook"},
			Listen:    map[string]string{"http": "127.0.0.1:0"},
			NoJobs:    true, NoStream: true,
			Ready: func(a map[string]string) { ready <- a },
		})
	}()
	select {
	case a := <-ready:
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("serve did not stop")
			}
		})
		return a
	case err := <-done:
		cancel()
		t.Fatalf("serve exited: %v", err)
	case <-time.After(90 * time.Second):
		cancel()
		t.Fatal("serve did not start")
	}
	return nil
}

func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// wsStatus does a raw WebSocket upgrade and returns the HTTP status line code.
func wsStatus(t *testing.T, addr, path string, hdr map[string]string) int {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var key [16]byte
	_, _ = rand.Read(key[:])
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n",
		path, addr, base64.StdEncoding.EncodeToString(key[:]))
	for k, v := range hdr {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatalf("ws %s: %v", path, err)
	}
	var code int
	if _, err := fmt.Sscanf(line, "HTTP/1.1 %d", &code); err != nil {
		t.Fatalf("status line %q", line)
	}
	return code
}

func TestServeNotebookAuth(t *testing.T) {
	pythonEnv(t)
	root := governedProject(t)
	writeFile(t, filepath.Join(root, "notebooks", "hello.py"), "import marimo\n\napp = marimo.App()\n\n\n@app.cell\ndef _():\n    import marimo as mo\n\n    mo.md('hello')\n    return\n\n\nif __name__ == '__main__':\n    app.run()\n")
	tok := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "ana", "--project", root))
	t.Setenv("DUCT_NOTEBOOK_TOKEN", tok)
	addrs := startNotebookServe(t, root)
	nb := addrs["notebook"]
	if nb == "" || !strings.HasPrefix(nb, "127.0.0.1:") {
		t.Fatalf("notebook address %q", nb)
	}
	get := func(path string, hdr map[string]string) int {
		t.Helper()
		req, _ := http.NewRequest("GET", "http://"+nb+path, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	// The Python app takes a moment to import marimo: wait for an HTTP answer.
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := http.Get("http://" + nb + "/notebooks/hello")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("notebook app did not start: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Run("http", func(t *testing.T) {
		if c := get("/notebooks/hello", nil); c != 401 {
			t.Errorf("no credentials: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": "Bearer dct_nope"}); c != 401 {
			t.Errorf("bad token: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": basic("ana", "wrong")}); c != 401 {
			t.Errorf("bad password: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": basic("nadie", "pw-nadie-123")}); c != 401 {
			t.Errorf("valid user of another role: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": basic("baja", "pw-baja-123")}); c != 401 {
			t.Errorf("disabled user: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": basic("ana", "pw-ana-123")}); c != 200 {
			t.Errorf("valid user: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": "Bearer " + tok}); c != 200 {
			t.Errorf("valid token: %d", c)
		}
		if c := get("/notebooks/hello?token="+tok, nil); c != 200 {
			t.Errorf("token in query: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Cookie": "duct_nb=" + tok}); c != 200 {
			t.Errorf("cookie: %d", c)
		}
	})

	t.Run("websocket", func(t *testing.T) {
		// marimo executes code over the websocket: it must never open without credentials.
		for _, path := range []string{"/notebooks/hello/ws?session_id=s1", "/ws", "/notebooks/hello/ws_sync"} {
			if c := wsStatus(t, nb, path, nil); c != 403 {
				t.Errorf("%s without token: %d, want 403", path, c)
			}
			if c := wsStatus(t, nb, path, map[string]string{"Authorization": "Bearer dct_nope"}); c != 403 {
				t.Errorf("%s bad token: %d, want 403", path, c)
			}
			if c := wsStatus(t, nb, path, map[string]string{"Authorization": basic("nadie", "pw-nadie-123")}); c != 403 {
				t.Errorf("%s other role: %d, want 403", path, c)
			}
			if c := wsStatus(t, nb, path, map[string]string{"Authorization": "Bearer " + tok, "Origin": "http://evil.example"}); c != 403 {
				t.Errorf("%s cross-origin: %d, want 403", path, c)
			}
		}
		// With credentials the guard lets it through to marimo (which answers 101 or its own error).
		c := wsStatus(t, nb, "/notebooks/hello/ws?session_id=s1", map[string]string{"Authorization": "Bearer " + tok})
		t.Logf("authenticated websocket upgrade: %d", c)
		if c != 101 {
			t.Errorf("authenticated websocket: %d, want 101", c)
		}
	})

	t.Run("guards", func(t *testing.T) {
		// Failures are rate limited per user.
		var last int
		for range 8 {
			last = get("/notebooks/hello", map[string]string{"Authorization": basic("ana", hex.EncodeToString([]byte(time.Now().String())))})
		}
		_ = last
		// (a differently spelled header is a different cache key at the Python side)
		again := strings.Replace(basic("ana", "pw-ana-123"), "Basic", "basic", 1)
		if c := get("/notebooks/hello", map[string]string{"Authorization": again}); c != 401 {
			t.Errorf("after repeated failures even the right password waits: %d", c)
		}
		if c := get("/notebooks/hello", map[string]string{"Authorization": "Bearer " + tok}); c != 200 {
			t.Errorf("tokens are not throttled by password failures: %d", c)
		}
	})
}
