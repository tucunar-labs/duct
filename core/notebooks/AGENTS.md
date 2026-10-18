# notebooks

- **Purpose:** the notebook service of the web console (ADR 0006, "Implementado 2026-10-18"): files of
  `<project>/notebooks/*.py`, one marimo process per (user, notebook, mode), exclusive edit lock per file, version
  history with diffs and restore, comments, create / rename / delete. Implements `contract.NotebooksConsole`;
  `core/governance` wraps it to audit (`NOTEBOOK ...`), `modules/web` renders it and proxies `/nb/...`.
- **Config** (`[notebooks]` in `duct.toml`, `core/project/notebooksconfig.go`): `editors` (roles; default `duct_admin`),
  `idle_timeout` (30m), `max_age` (12h, also the token's lifetime), `max_kernels` (8).
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
  and diffs are editors-only because they show code. Known limit: editors run Python as the server's OS user and the
  notebook shares the console's origin (see the ADR).
- **Tests:** `diff_test.go` (pure); the service against a real engine and the console in `cmd/duct/notebooks_console_test.go`.
