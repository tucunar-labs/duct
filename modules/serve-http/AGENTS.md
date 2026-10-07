# serve-http

- **Purpose:** HTTP `Protocol` of `duct serve` with the ClickHouse HTTP API shape (ADR 0008).
  Columnar path for BI, notebooks and user code.
- **Contract:** `core/contract/v1` `Protocol` (+ optional `Addresser`); registers as
  `protocol/http`. Never touches data except through the `QueryHandler`, which runs the
  statement as the authenticated identity over its role views (single SELECT, role schema only,
  no table functions; see `core/governance`).
- **Config** (`[serve.http]`): `listen` (default `127.0.0.1:8123`, `:0` = random port),
  `tls_cert` + `tls_key` (both or none), `max_concurrent` (default 32).
- **API:** `POST /query` (also `/`) with the SQL as body, or `?query=`. Format: trailing
  `FORMAT JSON|JSONEachRow|CSV|CSVWithNames|ArrowStream|Parquet` or `?default_format=`
  (default JSON). `GET /healthz`, `GET /metrics` (Prometheus text, no auth).
- **Auth:** `Authorization: Bearer <token>` (`duct token create`), or Basic / `X-ClickHouse-User`
  + `X-ClickHouse-Key` with user and password. 401 on failure, 403 when governance refuses the
  statement, 400 for SQL errors, 429 over `max_concurrent`.
- **Gotchas:** an error after the first byte cuts the connection (status already sent). JSON
  64-bit ints are plain numbers (ClickHouse quotes them).
- **Tests:** `http_test.go` with a fake handler and auth; end-to-end in `cmd/duct/serve_test.go`.
- **Status:** experimental.
