# task-python

- **Purpose:** TaskType `python` of the orchestrator (ADR 0005): `python3 script.py args...` or
  `uv run python script.py args...` in the project.
- **Contract:** `core/contract/v1` `TaskType`; registers as `task/python`.
- **Params:** `script` (required), `args` (list), `uv` (bool), `python` (interpreter), `cwd`, `env`.
- **Credentials:** the child gets `DUCT_URL` and `DUCT_TOKEN` (duct serve, ADR 0007) so
  `duct.connect()` works; never the lake or catalog credentials (the orchestrator builds the
  environment, see `core/contract/v1/taskrun`).
- **Status:** experimental.
