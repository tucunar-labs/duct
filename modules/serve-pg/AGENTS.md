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
- **Config** (`[serve.pg]`): `listen` (default `127.0.0.1:5433`), `tls_cert`+`tls_key`
  (when set, plaintext logins are refused), `allow_insecure` (needed for non-loopback without TLS),
  `max_connections` (32), `idle_timeout` (15m), `memory_limit` (256MB/session), `threads` (2).
  `catalog` and `data_path` are injected by `duct serve`. Local data path only for now.
- **Gotchas:** 10 bad logins from one IP in 5 min ban it; duckgres logs per statement at INFO
  (`duct serve` raises the default level to WARN). Depends on `../duckgres` (go.mod `replace`).
- **Tests:** end-to-end in `cmd/duct/serve_test.go` (pgx over TLS).
- **Status:** experimental.
