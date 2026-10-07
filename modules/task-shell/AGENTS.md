# task-shell

- **Purpose:** TaskType `shell` of the orchestrator (ADR 0005): runs a program with an argument
  vector, never through a shell, so there is no injection surface.
- **Contract:** `core/contract/v1` `TaskType`; registers as `task/shell`. Process handling is
  `core/contract/v1/taskrun` (process group killed on cancel, output tail of 4 KiB).
- **Params:** `command` (list, required), `cwd` (relative to the project), `env` (map). The
  orchestrator passes the whole child environment (allow-list + `DUCT_URL`/`DUCT_TOKEN` + `env`);
  lake and catalog credentials are never in it.
- **Status:** experimental.
