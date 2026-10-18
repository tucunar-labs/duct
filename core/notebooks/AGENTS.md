# notebooks

- **Purpose:** the notebook service of the web console (ADR 0006, "Implementado 2026-10-18"): files of
  `<project>/notebooks/*.py`, one marimo process per (user, notebook, mode), exclusive edit lock per file, version
  history with diffs and restore, comments, create / rename / delete. Implements `contract.NotebooksConsole`;
  `core/governance` wraps it to audit (`NOTEBOOK ...`), `modules/web` renders it and proxies `/nb/...`.
- **Config** (`[notebooks]` in `duct.toml`, `core/project/notebooksconfig.go`): `editors` (roles; default `duct_admin`),
  `idle_timeout` (30m), `max_age` (12h, also the token's lifetime), `max_kernels` (8), `sandbox` (`auto` | `bwrap` | `none`).
- **Sandbox** (`sandbox.go`): with `bwrap` every process runs inside bubblewrap: system dirs and the Python venv read-only, private
  tmpfs `/tmp` and home, own `/proc`, `notebooks/` the only project path (rw in edit mode, ro in run mode), no lake, no `.duct/`,
  no `duct.toml`, network shared (needs DUCT_URL on loopback), `--die-with-parent --new-session --cap-drop ALL`. `auto` without a
  working bwrap: `cmd/duct/notebooks_serve.go` restricts editors to `duct_admin`. Never bind a path that contains the project root.
- **Processes** (`kernel.go`, `launch.go`): `python -m marimo edit|run --headless --host 127.0.0.1 --port <free>
  --base-url /nb/<mode>/<name> --token-password-file -` (password random per process, via stdin), cwd = project root,
  environment = `udf.SafeEnv` + `DUCT_URL` + `DUCT_TOKEN` (token of the user, label `notebook <name> (<mode>)`, revoked on
  stop). `Launcher` is an interface so tests use a fake. Stop = SIGTERM to the process group, SIGKILL after 5 s; marimo's
  kernel child lives in its own group but exits with its parent. The janitor (every `min(2s, idle/3)`) kills idle / too old
  processes and, for edit processes, records saves by stat + hash.
- **Lock:** the edit process is the lock (in memory: per `duct serve`). `takeFile` frees a file for rename / delete /
  restore (another user's edit is `ErrNotebookLocked`; the own one is saved and stopped; run processes are stopped).
- **State:** migration 12, `duct_notebook_versions` (content included) and `duct_notebook_comments`; instants are epoch
  microseconds. Names are `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (also the guard against path traversal).
- **Security:** authorization lives here (editor roles, admin-only delete, comment resolve = author or editor); history
  and diffs are editors-only because they show code. The notebooks have their own origin
  (`modules/web/nbauth.go`). Residual risks (editor code runs with the viewer's token, jobs' `notebook` tasks unsandboxed): see the ADR.
- **Tests:** `diff_test.go` (pure), `sandbox_test.go` (real bwrap, skipped without it); the service against a real engine and the console in `cmd/duct/notebooks_console_test.go`.
