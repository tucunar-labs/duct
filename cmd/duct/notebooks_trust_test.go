package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/notebooks"
	"github.com/alonsoburon/duct/core/project"
)

var hashField = regexp.MustCompile(`name="hash" value="([0-9a-f]{64})"`)

// TestNotebooksAuthorTrust: the code of a notebook runs with the least privilege of the people who wrote it
// (ADR 0006). An analyst edits, an admin opens: the kernel holds the analyst's token and the page says so; after
// "Confiar en esta versión" it holds the admin's and the trust is audited; the analyst's next edit revokes it.
func TestNotebooksAuthorTrust(t *testing.T) {
	s := startNotebookConsole(t, "10m")
	ana := s.login(t, "ana", "pw-ana-123")
	bea := s.login(t, "bea", "pw-bea-123")
	jefa := s.login(t, "jefa", "pw-jefa-123")
	nadie := s.login(t, "nadie", "pw-nadie-123")
	path := filepath.Join(s.root, "notebooks", "plan.py")

	// Which rights a process token carries: analistas sees the CL rows with the rut masked, the admin everything.
	rights := func(tok string) string {
		req, _ := http.NewRequest("POST", "http://"+s.http+"/query", strings.NewReader("SELECT name, rut FROM customers ORDER BY name FORMAT CSV"))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		switch {
		case resp.StatusCode != 200:
			return "denied"
		case strings.Contains(string(b), "Beto") && strings.Contains(string(b), "11.111.111-1"):
			return "admin"
		case strings.Contains(string(b), "***") && !strings.Contains(string(b), "Beto"):
			return "analistas"
		}
		return "other: " + string(b)
	}
	// ana writes the notebook (a create plus a save by her editor).
	if resp, _ := ana.post("/notebooks", url.Values{"name": {"plan"}}); resp.StatusCode != 303 {
		t.Fatalf("create: %d", resp.StatusCode)
	}
	if code, _, _ := ana.get("/notebooks/plan"); code != 200 {
		t.Fatalf("ana opens for edit: %d", code)
	}
	b, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(b, []byte("# exfiltrate()\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	nbWait(t, "ana's save", 20*time.Second, func() bool {
		rows := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_notebook_versions WHERE notebook = 'plan' AND author = 'ana' AND action = 'save'")
		return rows[0]["n"].(float64) >= 1
	})
	if resp, _ := ana.post("/logout", nil); resp.StatusCode != 303 {
		t.Fatalf("logout: %d", resp.StatusCode)
	}
	ana = s.login(t, "ana", "pw-ana-123")

	t.Run("admin reading analyst code runs with the analyst's role", func(t *testing.T) {
		_, body, _ := jefa.get("/notebooks/plan?mode=run")
		if !strings.Contains(body, "corre con los permisos de") || !strings.Contains(body, "<strong>ana</strong>") || !strings.Contains(body, "Confiar en esta versión") {
			t.Fatalf("no banner: %s", textOnly(body))
		}
		p := s.launcher.last("run", "plan")
		if p == nil || rights(p.spec.Env["DUCT_TOKEN"]) != "analistas" {
			t.Fatalf("the admin's run kernel does not hold the analyst's token: %v", p)
		}
		// edit mode obeys the same rule
		_, body, _ = jefa.get("/notebooks/plan?mode=edit")
		if !strings.Contains(body, "corre con los permisos de") {
			t.Errorf("edit mode has no banner: %s", textOnly(body))
		}
		if e := s.launcher.last("edit", "plan"); e == nil || rights(e.spec.Env["DUCT_TOKEN"]) != "analistas" {
			t.Errorf("the admin's edit kernel does not hold the analyst's token")
		}
		rows := queryRows(t, s.root, "SELECT user_name, label FROM duct.duct_tokens WHERE label LIKE 'notebook plan (run) opened by jefa' AND NOT revoked")
		if len(rows) != 1 || rows[0]["user_name"] != "ana" {
			t.Errorf("token label/owner = %v", rows)
		}
	})

	t.Run("trust gives the code the admin's role, a new edit takes it back", func(t *testing.T) {
		_, body, _ := jefa.get("/notebooks/plan?mode=run")
		m := hashField.FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("no trust form: %s", textOnly(body))
		}
		// a hash that is not the current content is refused
		resp, _ := jefa.post("/notebooks/plan/trust", url.Values{"hash": {strings.Repeat("0", 64)}})
		if b := jefa.follow(resp); !strings.Contains(flashText(b), "cambió") && !strings.Contains(flashText(b), "changed") {
			t.Errorf("a wrong hash was accepted: %s", flashText(b))
		}
		resp, _ = jefa.post("/notebooks/plan/trust", url.Values{"hash": {m[1]}})
		if b := jefa.follow(resp); !strings.Contains(b, "Confías en esta versión") {
			t.Fatalf("trust: %s", flashText(b))
		}
		_, body, _ = jefa.get("/notebooks/plan?mode=run")
		if strings.Contains(body, "corre con los permisos de") || !strings.Contains(body, "Confiaste en esta versión") {
			t.Errorf("banner after trusting: %s", textOnly(body))
		}
		if p := s.launcher.last("run", "plan"); rights(p.spec.Env["DUCT_TOKEN"]) != "admin" {
			t.Errorf("after trusting, the kernel does not hold the admin's token")
		}
		_, aud, _ := jefa.get("/admin/audit?protocol=web")
		if !strings.Contains(aud, "NOTEBOOK trust name=plan hash="+m[1][:12]) || !strings.Contains(aud, "runas=ana") {
			t.Errorf("the trust or the limited open is not in the audit log")
		}

		// ana edits again (jefa's edit session of the previous subtest holds the lock: she leaves first)
		if resp, _ := jefa.post("/logout", nil); resp.StatusCode != 303 {
			t.Fatalf("logout: %d", resp.StatusCode)
		}
		jefa = s.login(t, "jefa", "pw-jefa-123")
		if code, _, _ := ana.get("/notebooks/plan"); code != 200 {
			t.Fatal("ana reopens")
		}
		b, _ := os.ReadFile(path)
		if err := os.WriteFile(path, append(b, []byte("# exfiltrate_again()\n")...), 0o600); err != nil {
			t.Fatal(err)
		}
		nbWait(t, "ana's second save", 20*time.Second, func() bool {
			rows := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_notebook_versions WHERE notebook = 'plan' AND author = 'ana' AND action = 'save'")
			return rows[0]["n"].(float64) >= 2
		})
		_, body, _ = jefa.get("/notebooks/plan?mode=run")
		if !strings.Contains(body, "corre con los permisos de") {
			t.Fatalf("a new edit did not revoke the trust: %s", textOnly(body))
		}
		if p := s.launcher.last("run", "plan"); rights(p.spec.Env["DUCT_TOKEN"]) != "analistas" {
			t.Errorf("the run kernel was not replaced after the edit")
		}
	})

	t.Run("same role runs as itself, another role must trust", func(t *testing.T) {
		// bea has ana's role: nothing to escalate
		_, body, _ := bea.get("/notebooks/plan?mode=run")
		if strings.Contains(body, "corre con los permisos de") || strings.Contains(body, "no se abre hasta") {
			t.Fatalf("same-role reader got a barrier: %s", textOnly(body))
		}
		if p := s.launcher.last("run", "plan"); p == nil || rights(p.spec.Env["DUCT_TOKEN"]) != "analistas" {
			t.Errorf("bea's kernel: %v", p)
		}
		// nadie (role vacio) is not comparable with ana's: neither her rights nor nadie's are safe to give
		before := len(s.launcher.all())
		_, body, _ = nadie.get("/notebooks/plan")
		if !strings.Contains(body, "no se abre hasta que confíes") || strings.Contains(body, "<iframe") {
			t.Fatalf("incomparable role: %s", textOnly(body))
		}
		if got := len(s.launcher.all()); got != before {
			t.Errorf("a process started for a reader that has to trust first")
		}
		m := hashField.FindStringSubmatch(body)
		if m == nil {
			t.Fatal("no trust form for nadie")
		}
		resp, _ := nadie.post("/notebooks/plan/trust", url.Values{"hash": {m[1]}})
		_ = nadie.follow(resp)
		if _, body, _ = nadie.get("/notebooks/plan"); !strings.Contains(body, "<iframe") {
			t.Errorf("after trusting, nadie still cannot open: %s", textOnly(body))
		}
		if p := s.launcher.last("run", "plan"); rights(p.spec.Env["DUCT_TOKEN"]) != "denied" {
			t.Errorf("nadie's kernel must hold nadie's own (empty) rights")
		}
	})

	t.Run("an admin's own save is a review", func(t *testing.T) {
		if resp, _ := ana.post("/logout", nil); resp.StatusCode != 303 { // ana's edit session holds the lock
			t.Fatalf("logout: %d", resp.StatusCode)
		}
		// jefa edits (as ana's rights, banner) and saves: her version is the newest, so the admin role reviewed it
		if code, _, _ := jefa.get("/notebooks/plan?mode=edit"); code != 200 {
			t.Fatal("jefa edits")
		}
		b, _ := os.ReadFile(path)
		if err := os.WriteFile(path, append(b, []byte("# reviewed by jefa\n")...), 0o600); err != nil {
			t.Fatal(err)
		}
		nbWait(t, "jefa's save", 20*time.Second, func() bool {
			rows := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_notebook_versions WHERE notebook = 'plan' AND author = 'jefa'")
			return rows[0]["n"].(float64) >= 1
		})
		if _, body, _ := jefa.get("/notebooks/plan?mode=run"); strings.Contains(body, "corre con los permisos de") {
			t.Errorf("an admin's own save did not count as a review: %s", textOnly(body))
		}
		if p := s.launcher.last("run", "plan"); rights(p.spec.Env["DUCT_TOKEN"]) != "admin" {
			t.Errorf("run kernel after the admin's save does not hold the admin's token")
		}
	})
}

// An editor's code can write every notebook of the folder (the edit sandbox mounts it), not only the one open: the
// change is attributed to the live editor, so an admin opening the other notebook gets the same rule.
func TestNotebooksSandboxWriteIsAttributed(t *testing.T) {
	s := startNotebookConsole(t, "10m")
	ana := s.login(t, "ana", "pw-ana-123")
	jefa := s.login(t, "jefa", "pw-jefa-123")
	if resp, _ := ana.post("/notebooks", url.Values{"name": {"plan"}}); resp.StatusCode != 303 {
		t.Fatalf("create: %d", resp.StatusCode)
	}
	hello := filepath.Join(s.root, "notebooks", "hello.py")
	if code, _, _ := jefa.get("/notebooks/hello?mode=run"); code != 200 { // a first look: nothing attributed yet
		t.Fatal("jefa opens hello")
	}
	if code, _, _ := ana.get("/notebooks/plan"); code != 200 {
		t.Fatal("ana edits plan")
	}
	b, _ := os.ReadFile(hello)
	if err := os.WriteFile(hello, append(b, []byte("# written by ana's code in the sandbox\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	nbWait(t, "the change attributed to ana", 20*time.Second, func() bool {
		rows := queryRows(t, s.root, "SELECT count(*) AS n FROM duct.duct_notebook_versions WHERE notebook = 'hello' AND author = 'ana'")
		return rows[0]["n"].(float64) >= 1
	})
	if _, body, _ := jefa.get("/notebooks/hello?mode=run"); !strings.Contains(body, "corre con los permisos de") || !strings.Contains(body, "<strong>ana</strong>") {
		t.Errorf("the admin ran a notebook an editor's code rewrote with her own rights: %s", textOnly(body))
	}
}

// jobIdentity logs the CLI user in to the engine: a token for user.
func jobToken(t *testing.T, root, user string) string {
	t.Helper()
	out := mustDuct(t, "token", "create", "--user", user, "--project", root)
	for _, f := range strings.Fields(out) {
		if strings.HasPrefix(f, "dtk_") || strings.HasPrefix(f, "duct_") {
			return f
		}
	}
	return strings.TrimSpace(strings.Split(strings.TrimSpace(out), "\n")[0])
}

// TestNotebookJobGuard: a scheduled notebook task fails closed when an author with another role changed the
// notebook after the job identity last reviewed it; `duct notebook approve` is the review.
func TestNotebookJobGuard(t *testing.T) {
	root := governedProject(t)
	mustDuct(t, "sql", "--project", root, "CREATE ROLE duct_admin")
	mustDuct(t, "user", "create", "jefa", "--role", "duct_admin", "--password", "pw-jefa-123", "--project", root)
	tok := jobToken(t, root, "jefa")
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := project.Migrate(ctx, eng); err != nil {
		t.Fatal(err)
	}
	guard := notebookGuard(eng, tok)
	nb := filepath.Join(root, "notebooks", "etl.py")
	version := func(author, content string) {
		t.Helper()
		writeFile(t, nb, content)
		if err := eng.Exec(ctx, `INSERT INTO duct.duct_notebook_versions (id, notebook, content_hash, author, action, created_at_us, size_bytes, lines_added, lines_removed, content)
			VALUES (?, 'etl', ?, ?, 'save', ?, ?, 0, 0, ?)`, time.Now().UnixNano(), notebooks.HashOf([]byte(content)), author, time.Now().UnixMicro(), int64(len(content)), content); err != nil {
			t.Fatal(err)
		}
	}
	// repository code: no history, nothing to check
	writeFile(t, nb, "print('v0')\n")
	if err := guard(ctx, "j", "t", nb); err != nil {
		t.Fatalf("a notebook Duct never saw: %v", err)
	}
	version("jefa", "print('v1 by an admin')\n")
	if err := guard(ctx, "j", "t", nb); err != nil {
		t.Fatalf("the admin's own notebook: %v", err)
	}
	version("ana", "print('v2 by an analyst')\n")
	err = guard(ctx, "j", "t", nb)
	if err == nil || !strings.Contains(err.Error(), "ana") || !strings.Contains(err.Error(), "duct notebook approve etl --user jefa") {
		t.Fatalf("an analyst's edit did not stop the job: %v", err)
	}
	mustDuct(t, "notebook", "approve", "etl", "--user", "jefa", "--project", root)
	if err := guard(ctx, "j", "t", nb); err != nil {
		t.Fatalf("after the approval: %v", err)
	}
	version("ana", "print('v3 by an analyst again')\n")
	if err := guard(ctx, "j", "t", nb); err == nil {
		t.Fatal("a new edit after the approval did not stop the job")
	}
	// the approval is audited by the CLI; an unknown user is refused
	if out, _, code := duct(t, "notebook", "approve", "etl", "--user", "nobody", "--project", root); code == 0 {
		t.Fatalf("approve for an unknown user: %s", out)
	}
	_ = eng.Close()

	// end to end: the orchestrator refuses before running anything
	writeFile(t, filepath.Join(root, "jobs", "nb.yaml"), "name: nb\ntasks:\n  - {id: n, type: notebook, params: {notebook: notebooks/etl.py}}\n")
	t.Setenv("DUCT_TOKEN", tok)
	out, errs, code := duct(t, "jobs", "run", "nb", "--project", root)
	if code == 0 || !strings.Contains(out+errs, "duct notebook approve etl") {
		t.Fatalf("job run with an unreviewed notebook: code=%d %s %s", code, out, errs)
	}
	mustDuct(t, "notebook", "approve", "etl", "--user", "jefa", "--project", root)
	if out, errs, code := duct(t, "jobs", "run", "nb", "--project", root); code != 0 {
		t.Fatalf("job run after the approval: code=%d %s %s", code, out, errs)
	}
}

// TestJobSandbox: python tasks run inside bubblewrap: no .duct/, no local lake, no duct.toml, a read-only project
// with one explicit writable directory, and DUCT_URL still reaches the governed server.
func TestJobSandbox(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("no bubblewrap")
	}
	s := startNotebookConsole(t, "10m") // a serve with the HTTP protocol
	p, err := project.Load(s.root)
	if err != nil {
		t.Fatal(err)
	}
	mustDuct(t, "user", "create", "robot", "--role", "analistas", "--password", "pw-robot-123", "--project", s.root)
	tok := jobToken(t, s.root, "robot")
	writeFile(t, filepath.Join(s.root, "scripts", "probe.py"), `import json, os, urllib.request

def probe(p):
    try:
        if os.path.isdir(p):
            return "dir:" + ",".join(sorted(os.listdir(p)))
        with open(p, "rb") as f:
            return "file:" + str(len(f.read()))
    except Exception as e:
        return "ERR:" + type(e).__name__

r = {"state": probe(".duct"), "lake": probe("`+filepath.Base(p.DataPath())+`"), "toml": probe("duct.toml"), "scripts": probe("scripts")}
req = urllib.request.Request(os.environ["DUCT_URL"] + "/query", data=b"SELECT name FROM customers ORDER BY name FORMAT CSV",
                             headers={"Authorization": "Bearer " + os.environ["DUCT_TOKEN"]})
r["query"] = urllib.request.urlopen(req).read().decode().strip().replace("\n", ",")
for name in ("evil.txt", "scripts/evil.txt"):
    try:
        open(name, "w").write("x")
        r["write " + name] = "WROTE"
    except Exception as e:
        r["write " + name] = "ERR"
os.makedirs("work", exist_ok=True)
open("work/out.json", "w").write(json.dumps(r))
`)
	writeFile(t, filepath.Join(s.root, "jobs", "probe.yaml"), "name: probe\ntasks:\n  - id: p\n    type: python\n    params: {script: scripts/probe.py, writable: work}\n")
	writeFile(t, filepath.Join(s.root, "jobs", "open.yaml"), "name: open\ntasks:\n  - id: p\n    type: python\n    params: {script: scripts/probe.py, writable: work, sandbox: false}\n")
	t.Setenv("DUCT_URL", "http://"+s.http)
	t.Setenv("DUCT_TOKEN", tok)

	read := func() map[string]string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(s.root, "work", "out.json"))
		if err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(filepath.Join(s.root, "work", "out.json"))
		var m map[string]string
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if out, errs, code := duct(t, "jobs", "run", "probe", "--project", s.root); code != 0 {
		t.Fatalf("sandboxed job: code=%d %s %s", code, out, errs)
	}
	got := read()
	if got["state"] != "dir:" || got["lake"] != "dir:" || (got["toml"] != "file:0" && got["toml"] != "ERR:PermissionError") {
		t.Errorf("the sandboxed task sees the project's secrets: %v", got)
	}
	if !strings.Contains(got["scripts"], "probe.py") || !strings.Contains(got["query"], "Ana") || strings.Contains(got["query"], "Beto") {
		t.Errorf("scripts/ must be visible and DUCT_URL must reach the governed server as the job's role: %v", got)
	}
	if got["write evil.txt"] != "ERR" || got["write scripts/evil.txt"] != "ERR" {
		t.Errorf("the sandboxed task wrote into the project: %v", got)
	}
	// the opt-out runs it as the server's user
	if out, errs, code := duct(t, "jobs", "run", "open", "--project", s.root); code != 0 {
		t.Fatalf("unsandboxed job: code=%d %s %s", code, out, errs)
	}
	got = read()
	if got["state"] == "dir:" || got["write evil.txt"] != "WROTE" {
		t.Errorf("sandbox: false still isolated the task: %v", got)
	}
	_ = os.Remove(filepath.Join(s.root, "evil.txt"))
	_ = os.Remove(filepath.Join(s.root, "scripts", "evil.txt"))

	// [sandbox] jobs = "none" turns the default off; "bwrap" is explicit
	cfg := filepath.Join(s.root, "duct.toml")
	b, _ := os.ReadFile(cfg)
	writeFile(t, cfg, string(b)+"\n[sandbox]\njobs = \"none\"\n")
	writeFile(t, filepath.Join(s.root, "jobs", "probe.yaml"), "name: probe\ntasks:\n  - id: p\n    type: python\n    params: {script: scripts/probe.py, writable: work}\n")
	if out, errs, code := duct(t, "jobs", "run", "probe", "--project", s.root); code != 0 {
		t.Fatalf("job with sandbox none: code=%d %s %s", code, out, errs)
	}
	if got = read(); got["state"] == "dir:" {
		t.Errorf("[sandbox] jobs = none did not turn the isolation off: %v", got)
	}
	_ = os.Remove(filepath.Join(s.root, "evil.txt"))
	_ = os.Remove(filepath.Join(s.root, "scripts", "evil.txt"))
}
