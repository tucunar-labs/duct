# notebook

- **Not the console's notebooks:** the Notebooks section of the web console (edit mode, history, comments) is
  `core/notebooks` + `modules/web` (ADR 0006, "Implementado 2026-10-18"). This protocol is the older, run-only mount with its
  own loopback listener and one shared identity; it stays for deployments that want notebooks without the console.
- **Purpose:** `Protocol` of `duct serve` that serves the project's marimo notebooks (ADR 0006)
  as a web app, behind Duct authentication. Not started by default: name it in
  `[serve].protocols` or `--protocols`.
- **Contract:** `core/contract/v1` `Protocol` (+ `Addresser`); registers as `protocol/notebook`.
- **How:** it supervises `python -m duct.notebook.asgi` (SDK, `duct-data[marimo]`), which mounts
  `marimo.create_asgi_app().with_dynamic_directory(...)` (read-only run mode: visitors run the
  project's notebooks, not their own code) under a **pure ASGI** middleware that covers `http`
  and `websocket` (marimo executes code over the websocket). The middleware forwards the
  credentials (`Authorization: Bearer|Basic`, cookie `duct_nb`, `?token=`) to a loopback `/auth`
  endpoint of this module, guarded by a per-process secret, which calls the `AuthProvider`.
  Failures: 401 for http, close before accept (= HTTP 403) for websocket. Five failures per
  user in a minute -> 429.
- **Config** (`[serve.notebook]`): `listen` (loopback only, default `127.0.0.1:8124`), `dir`
  (`duct serve` sets `<project>/notebooks`), `role` (the only role allowed in and the identity the
  notebooks query as; default `duct_admin`), `token_env` (default `DUCT_NOTEBOOK_TOKEN`: a token
  of a user of that role, handed to the notebooks as `DUCT_TOKEN`), `python` (argv prefix;
  `duct serve` fills it from `[python]`), `duct_url` (filled with the HTTP protocol's URL).
- **Security:** notebooks get `DUCT_URL`/`DUCT_TOKEN` only (allow-listed environment; no lake
  or catalog credentials). One identity for all visitors, hence a single allowed role (no
  privilege difference between visitors and the notebook identity). TLS and public exposure:
  reverse proxy in front (Host/Origin must match, the middleware checks it).
- **Tests:** `cmd/duct/notebook_test.go` (end to end, needs `python/.venv`); Python side in
  `python/duct-data/tests/test_asgi.py` (http + websocket, 403 without token).
- **Status:** experimental.
