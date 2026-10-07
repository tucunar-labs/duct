# serve-pg

- **Purpose:** Postgres wire `Protocol` of `duct serve` on the duckgres fork (ADR 0008):
  psql, JDBC, ODBC, Power BI. Login with a Duct user (cleartext password message, so use TLS
  off loopback) and see only the views of the role (`role_<name>` schema).
- **Contract:** `core/contract/v1` `Protocol` (+ `Addresser`); registers as `protocol/pg`.
  Uses the optional `Auditor` of the handler to record every statement in `duct.duct_audit`.
- **Why it has its own DuckDB:** duckgres needs a `*sql.DB` (pg_catalog emulation installs
  per-session TEMP macros/views), so it cannot run through `QueryHandler`. Each login opens a
  private in-process DuckDB: lake attached **read only** as `ducklake`, the Duct state database
  (hashes, tokens) is never attached, `allow_persistent_secrets=false` before the first ATTACH,
  then ADR 0007's lockdown (allowed_directories, enable_external_access=false, no autoinstall/
  autoload, lock_configuration=true). Statements pass duckgres' `QueryAccessPolicy` (read only,
  role schema only, bare names resolved into it, `sqlite_*`/`read_*`/ATTACH/SET refused).
- **Governance SQL:** statements of `core/governance/sql.go` (CREATE ROLE, GRANT, SHOW GRANTS,
  ALTER TABLE ... SET ROW FILTER ...) are not Postgres SQL, so the policy's `Intercept` hook (fork
  patch) asks the handler's optional `contract.Administrator` first: admin (`duct_admin`) runs them
  and gets a constant SELECT back (prefixed `/*duct:admin*/`, skipped by the audit hook because the
  original is audited by `intercept`); others get 42501.
- **Quotas** (ADR 0013): if the handler is a `contract.Limiter`, `quota.go` wraps the executor given to duckgres: one slot per statement until its row set closes (shared counter with HTTP/MCP/web), `max_rows` stops the row set with a 53400 error (the fork reports `SQLState()` of executor errors). Tested in `cmd/duct/serve_quota_test.go`.
- **Config** (`[serve.pg]`): `listen` (default `127.0.0.1:5433`), `tls_cert`+`tls_key`
  (when set, plaintext logins are refused), `allow_insecure` (needed for non-loopback without TLS),
  `max_connections` (32), `idle_timeout` (15m), `memory_limit` (256MB/session), `threads` (2).
  `catalog` and `data_path` are injected by `duct serve`. Works with an `s3://` lake too: each session gets the S3 temporary secret (read-only STS credentials scoped to the prefix when `role_arn` is set, renewed during the session), `httpfs`, and `allowed_directories=[s3://bucket/prefix/]`.
- **Gotchas:** 10 bad logins from one IP in 5 min ban it; duckgres logs per statement at INFO
  (`duct serve` raises the default level to WARN). Depends on `../duckgres` (go.mod `replace`).
- **Tests:** end-to-end in `cmd/duct/serve_test.go` (pgx over TLS).
- **Status:** experimental.
