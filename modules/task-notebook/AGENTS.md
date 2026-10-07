# task-notebook

- **Purpose:** TaskType `notebook` of the orchestrator (ADR 0005, 0006): runs a marimo notebook
  as a script, `python nb.py --key value`, args read with `mo.cli_args()`.
- **Contract:** `core/contract/v1` `TaskType`; registers as `task/notebook`.
- **Params:** `notebook` (required), `args` (map), `uv` (bool), `python`, `cwd`, `env`.
- **Credentials:** same as `task-python`: `DUCT_URL`/`DUCT_TOKEN` only.
- **Status:** experimental (idempotency is the user's, ADR 0005).
