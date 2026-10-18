package sandbox

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// sandboxProject is a project with secrets in the places a notebook must not reach.
func sandboxProject(t *testing.T) (root, nbDir string) {
	t.Helper()
	root = t.TempDir()
	for f, c := range map[string]string{
		"lake/secret.parquet": "lake-data", ".duct/state.db": "password-hashes", ".duct/web_session.key": "key",
		"duct.toml": "[secret]\n", "notebooks/a.py": "print(1)\n",
	} {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root, filepath.Join(root, "notebooks")
}

// sbRun runs `sh -c script` inside the sandbox and returns the combined output.
func sbRun(t *testing.T, root, nbDir string, writable bool, env []string, script string) (string, error) {
	t.Helper()
	bw, err := Available()
	if err != nil {
		t.Skip("bubblewrap not usable here: ", err)
	}
	sh, _ := exec.LookPath("sh")
	sp := Spec{Bwrap: bw, Python: sh, Root: root}
	if writable {
		sp.Writable = []string{nbDir}
	} else {
		sp.ReadOnly = []string{nbDir}
	}
	argv, err := Argv(sp, []string{sh, "-c", script})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // test
	cmd.Env = Env(append([]string{"PATH=/usr/bin:/bin"}, env...))
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestSandboxHidesTheProject(t *testing.T) {
	root, nb := sandboxProject(t)
	for _, f := range []string{"lake/secret.parquet", ".duct/state.db", ".duct/web_session.key", "duct.toml"} {
		if out, err := sbRun(t, root, nb, true, nil, "cat "+filepath.Join(root, f)); err == nil || strings.Contains(out, "lake-data") || strings.Contains(out, "password-hashes") {
			t.Errorf("read %s inside the sandbox: err=%v out=%q", f, err, out)
		}
	}
	out, err := sbRun(t, root, nb, true, nil, "ls -A "+root+"; ls -A "+nb+"; pwd; echo $HOME")
	if err != nil {
		t.Fatal(out, err)
	}
	if got := strings.Fields(out); strings.Join(got, " ") != "notebooks a.py "+nb+" /home/sandbox" {
		t.Errorf("the sandbox sees %q", got)
	}
	// ../ from the notebooks dir does not lead to the lake either
	if out, err := sbRun(t, root, nb, true, nil, "cat ../lake/secret.parquet"); err == nil {
		t.Errorf("relative path escape: %q", out)
	}
}

func TestSandboxNotebooksDirModes(t *testing.T) {
	root, nb := sandboxProject(t)
	if out, err := sbRun(t, root, nb, false, nil, "echo x > "+filepath.Join(nb, "new.py")); err == nil {
		t.Errorf("a run-mode process wrote in the notebooks dir: %q", out)
	}
	if out, err := sbRun(t, root, nb, false, nil, "echo x >> "+filepath.Join(nb, "a.py")); err == nil {
		t.Errorf("a run-mode process changed a notebook: %q", out)
	}
	if out, err := sbRun(t, root, nb, true, nil, "echo '# saved' >> "+filepath.Join(nb, "a.py")); err != nil {
		t.Fatalf("an edit-mode process cannot save: %v %s", err, out)
	}
	if b, _ := os.ReadFile(filepath.Join(nb, "a.py")); !strings.Contains(string(b), "# saved") {
		t.Errorf("the save did not reach the host: %q", b)
	}
	// the system is read-only too
	if out, err := sbRun(t, root, nb, true, nil, "touch /usr/x"); err == nil {
		t.Errorf("wrote to /usr: %q", out)
	}
}

func TestSandboxPrivateTmpAndProc(t *testing.T) {
	root, nb := sandboxProject(t)
	if out, err := sbRun(t, root, nb, true, nil, "echo mine > /tmp/other-user-secret && echo mine > $HOME/f"); err != nil {
		t.Fatal(out, err)
	}
	// another process (another user's notebook) has its own, empty /tmp and home
	out, err := sbRun(t, root, nb, true, nil, "ls -A /tmp $HOME; cat /tmp/other-user-secret 2>&1 | head -1")
	if err != nil && !strings.Contains(out, "No such file") {
		t.Fatal(out, err)
	}
	if strings.Contains(out, "mine") || strings.Contains(out, "other-user-secret\n") && !strings.Contains(out, "No such file") {
		t.Errorf("a second sandbox saw the first one's files: %q", out)
	}
	if _, err := os.Stat("/tmp/other-user-secret"); err == nil {
		t.Error("the sandbox's /tmp is the host's")
	}
	// only its own processes in /proc
	out, err = sbRun(t, root, nb, true, nil, "ls /proc | grep -c '^[0-9][0-9]*$'")
	if err != nil {
		t.Fatal(out, err)
	}
	if n := strings.TrimSpace(out); n != "1" && n != "2" && n != "3" && n != "4" {
		t.Errorf("/proc shows %s processes: the host's are visible", n)
	}
	// the environment is the allow-list plus what was asked
	out, err = sbRun(t, root, nb, true, []string{"DUCT_URL=http://127.0.0.1:1"}, "env")
	if err != nil {
		t.Fatal(out, err)
	}
	for _, v := range []string{"DUCT_URL=http://127.0.0.1:1", "HOME=/home/sandbox", "TMPDIR=/tmp"} {
		if !strings.Contains(out, v) {
			t.Errorf("env lacks %s", v)
		}
	}
}

func TestSandboxLoopbackNetwork(t *testing.T) {
	root, nb := sandboxProject(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		b := make([]byte, 64)
		n, _ := c.Read(b)
		got <- string(b[:n])
	}()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash")
	}
	out, err := sbRun(t, root, nb, true, nil, bash+" -c 'exec 3<>/dev/tcp/"+strings.Replace(ln.Addr().String(), ":", "/", 1)+"; echo hello-from-sandbox >&3'")
	if err != nil {
		t.Fatalf("the sandbox cannot reach loopback: %v %s", err, out)
	}
	select {
	case s := <-got:
		if !strings.Contains(s, "hello-from-sandbox") {
			t.Errorf("got %q", s)
		}
	case <-time.After(5 * time.Second):
		t.Error("nothing arrived")
	}
}

func TestPythonBindsRefuseTheProject(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "project")
	if err := os.MkdirAll(filepath.Join(parent, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	// a venv living in the project's parent directory would hand the whole tree to the sandbox
	if err := os.WriteFile(filepath.Join(parent, "pyvenv.cfg"), []byte("home = /usr/bin\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "bin", "python"), []byte("#!/bin/sh\n"), 0o755); err != nil { //nolint:gosec // test
		t.Fatal(err)
	}
	if _, err := pythonBinds(filepath.Join(parent, "bin", "python"), root); err == nil {
		t.Error("a venv containing the project root was accepted")
	}
	// a venv inside the project is fine, and only the venv is bound
	venv := filepath.Join(root, ".duct", "venv")
	if err := os.MkdirAll(filepath.Join(venv, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(venv, "pyvenv.cfg"), []byte("home = /usr/bin\n"), 0o600)
	_ = os.WriteFile(filepath.Join(venv, "bin", "python"), []byte("#!/bin/sh\n"), 0o755) //nolint:gosec // test
	b, err := pythonBinds(filepath.Join(venv, "bin", "python"), root)
	if err != nil || len(b) != 1 || b[0].dst != venv {
		t.Errorf("binds = %v, %v", b, err)
	}
}

func TestResolve(t *testing.T) {
	if m, fb, _, err := Resolve("none"); err != nil || m != None || fb {
		t.Error(m, fb, err)
	}
	if _, _, _, err := Resolve("bogus"); err == nil {
		t.Error("an unknown mode was accepted")
	}
	mode, fallback, _, err := Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if _, e := Available(); e == nil && (mode != Bwrap || fallback) {
		t.Errorf("bwrap works but auto gave %s fallback=%v", mode, fallback)
	} else if e != nil && (mode != None || !fallback) {
		t.Errorf("no bwrap: auto must report the fallback, got %s fallback=%v", mode, fallback)
	}
}

// projectRun runs a script inside the sandbox with the whole project read-only (job tasks, UDF workers).
func projectRun(t *testing.T, root string, writable []string, script string) (string, error) {
	t.Helper()
	bw, err := Available()
	if err != nil {
		t.Skip("bubblewrap not usable here: ", err)
	}
	sh, _ := exec.LookPath("sh")
	sp := Spec{Bwrap: bw, Python: sh, Root: root, ProjectRO: true, Hide: ProjectHide(root, filepath.Join(root, "lake")), Writable: writable}
	argv, err := Argv(sp, []string{sh, "-c", script})
	if err != nil {
		return "", err
	}
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // test
	cmd.Env = Env([]string{"PATH=/usr/bin:/bin"})
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestProjectReadOnlyHidesSecrets(t *testing.T) {
	root, _ := sandboxProject(t)
	for f, c := range map[string]string{".env": "TOKEN=x", ".git/config": "[core]", "functions/f.py": "pass", "work/keep": ""} {
		p := filepath.Join(root, f)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"lake/secret.parquet", ".duct/state.db", ".duct/web_session.key", "duct.toml", ".env", ".git/config"} {
		if out, err := projectRun(t, root, nil, "cat "+filepath.Join(root, f)); strings.Contains(out, "lake-data") || strings.Contains(out, "password-hashes") || strings.Contains(out, "TOKEN") || strings.Contains(out, "[core]") || strings.Contains(out, "[secret]") {
			t.Errorf("read %s: err=%v out=%q", f, err, out)
		}
	}
	// what is not secret is there, read-only; the hidden directories cannot be written either
	if out, err := projectRun(t, root, nil, "cat "+filepath.Join(root, "functions/f.py")+" && pwd"); err != nil || !strings.Contains(out, "pass") {
		t.Errorf("functions/ is not visible: %v %q", err, out)
	}
	for _, w := range []string{"functions/new.py", "notebooks/a.py", ".duct/x", "lake/x", "x"} {
		if out, err := projectRun(t, root, nil, "echo x > "+filepath.Join(root, w)); err == nil {
			t.Errorf("wrote %s in a read-only project: %q", w, out)
		}
	}
	// the explicit work directory is the only writable place
	if out, err := projectRun(t, root, []string{filepath.Join(root, "work")}, "echo ok > "+filepath.Join(root, "work/out.txt")); err != nil {
		t.Fatalf("the work dir is not writable: %v %s", err, out)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "work/out.txt")); !strings.Contains(string(b), "ok") {
		t.Errorf("the write did not reach the host: %q", b)
	}
	// a writable path may not contain what is hidden, nor be the root
	for _, w := range []string{root, filepath.Join(root, ".duct"), filepath.Join(root, ".duct", "sub")} {
		if _, err := projectRun(t, root, []string{w}, "true"); err == nil {
			t.Errorf("writable %s accepted", w)
		}
	}
}

func TestProjectHideList(t *testing.T) {
	root, _ := sandboxProject(t)
	h := ProjectHide(root, filepath.Join(root, "lake"), "s3://bucket/lake", "/elsewhere/lake")
	var rel []string
	for _, p := range h {
		r, _ := filepath.Rel(root, p)
		rel = append(rel, r)
	}
	if got := strings.Join(rel, " "); got != ".duct duct.toml lake" {
		t.Errorf("hide = %q", got)
	}
}
