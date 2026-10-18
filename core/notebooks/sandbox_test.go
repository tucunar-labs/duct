package notebooks

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
	bw, err := BwrapAvailable()
	if err != nil {
		t.Skip("bubblewrap not usable here: ", err)
	}
	sh, _ := exec.LookPath("sh")
	argv, err := bwrapArgv(sandboxSpec{Bwrap: bw, Python: []string{sh}, ProjectRoot: root, NotebooksDir: nbDir, Writable: writable}, []string{sh, "-c", script})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // test
	cmd.Env = sandboxEnv(append([]string{"PATH=/usr/bin:/bin"}, env...))
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

func TestResolveSandbox(t *testing.T) {
	if _, _, _, err := ResolveSandbox("none"); err != nil {
		t.Error(err)
	}
	mode, adminOnly, _, err := ResolveSandbox("")
	if err != nil {
		t.Fatal(err)
	}
	if _, e := BwrapAvailable(); e == nil && (mode != SandboxBwrap || adminOnly) {
		t.Errorf("bwrap works but auto gave %s adminOnly=%v", mode, adminOnly)
	} else if e != nil && (mode != SandboxNone || !adminOnly) {
		t.Errorf("no bwrap: auto must restrict editors, got %s adminOnly=%v", mode, adminOnly)
	}
}
