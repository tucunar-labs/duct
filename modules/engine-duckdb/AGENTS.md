# engine-duckdb

- **Purpose:** `contract.Engine` over DuckDB + DuckLake (ADR 0001, 0002, 0007). The only
  package that links cgo (`duckdb-go`).
- **Contract:** `core/contract/v1` `Engine`; registers as `engine/duckdb`.
- **Build:** needs `-tags duckdb_arrow` (Arrow results). `justfile` exports it in `GOFLAGS`;
  use `mise exec -- just ...` or export `GOFLAGS=-tags=duckdb_arrow`.
- **DuckDB version:** `duckdb-go v2.10506.0` = DuckDB **1.5.6** (stable). The 2.0 line
  (`v2.20000.0-N.preview`) still reports `v1.5.4` and its `ducklake` extension from the
  repository crashes on load; no extension build matches it. Move to 2.0 when `duckdb-go`
  ships a 2.0 tag whose version matches a published extension build (alpha repo is
  `v2.0.0-alpha<N>`): bump `duckdb-go` and `duckdb-go-bindings` together in `go.mod`.
- **Init config:** `catalog` (abs path, SQLite DuckLake metadata, **or** a Postgres
  connection string), `data_path` (abs dir or URL), `state` (abs path, SQLite file with
  Duct-owned tables, attached as `duct`; **or** a Postgres connection string, then schema
  `state_schema`, default `duct`), `locked` (bool), `busy_timeout_ms` (default 5000, SQLite
  only), `extensions` (extra extensions to INSTALL+LOAD; `catalog migrate` passes `postgres`).
- **Memory mode:** `Init` with `{"memory": true}` (and nothing else) opens a plain in-memory
  DuckDB: no lake, no Duct state, no extensions, default catalog `memory`. `duct alerts` keeps
  the stream windows there (`memory.go`).
- **Catalogs:** lake = `lake` (default after Init), Duct tables = `duct` (`duct.duct_*`).
- **Extensions:** `INSTALL` + `LOAD` of `ducklake` and `sqlite_scanner` at Init (network the
  first time; cached in `~/.duckdb/extensions/<version>/`).
- **Postgres catalog** (`postgres://user:pass@host:5432/db` or `postgres:dbname=... host=...
  user=... password=...`, parsed by `core/pgconn`): `ATTACH 'ducklake:postgres:' AS lake
  (DATA_PATH ..., META_SECRET duct_pg_catalog [, OVERRIDE_DATA_PATH true])` (DuckDB 1.5.6,
  `postgres` extension). The connection goes through `CREATE TEMPORARY SECRET (TYPE postgres,
  HOST, PORT, DATABASE, USER, PASSWORD, SSLMODE...)` because a plain DSN fails with
  `allow_persistent_secrets=false` ("Unknown secret storage: local_file"). DuckLake tables
  live in `public`; the Duct tables in schema `duct` of the same database (created on first
  use, `ATTACH '' (TYPE postgres, SECRET ..., SCHEMA 'duct')`), native BOOLEAN/TIMESTAMP.
  Errors are scrubbed of the password (DuckDB echoes the DSN). Locked mode works (the
  connection is open before the lock).
  **Concurrency:** many processes may write at once. DuckLake commits optimistically and
  retries snapshot-id races itself (`ducklake_max_retry_count`, raised to 100 here): 12
  processes x 8 appends of 20 000 rows all succeeded. A *logical* conflict is not retried
  and fails for the loser with `Transaction conflict - ...` (same table created twice, two
  `DELETE`/full-refresh on the same table, schema created twice); callers retry or avoid it
  (`ingest` retries its `CREATE SCHEMA IF NOT EXISTS`). First-time init races (schema, state
  tables) are retried in `Init`/`project.Migrate`. Prefer one `duct init`/`duct migrate`
  before starting several machines.
- **SQLite catalog:** DuckLake's ATTACH rejects `JOURNAL_MODE`, so Init puts the file in WAL
  through a plain sqlite attach first (persists in the header); `BUSY_TIMEOUT` goes to the
  DuckLake ATTACH. `OVERRIDE_DATA_PATH` is set for local paths so moving the project works.
  `PRAGMA wal_checkpoint` is only reachable via `sqlite_query()` on a plain sqlite attach
  (done by `core/project` GC).
- **Locked mode** (set after ATTACH): `allowed_directories=[data_path/]`,
  `enable_external_access=false`, autoinstall/autoload off, plus `allow_persistent_secrets=false` set before the first ATTACH (DuckDB refuses to change it once the secret manager was used), `lock_configuration=true`.
  Lake reads and writes (Parquet) keep working; `read_csv('/etc/passwd')`, `COPY ... TO`,
  `ATTACH`, `INSTALL` and `SET` of those options are denied. Spill-to-disk (`temp_directory`) is not
  allowed in locked mode.
- **S3 storage** (`data_path = s3://bucket/prefix`, key `s3` = `core/s3conn.Config`, built by
  `core/project` from `[s3]` + env): `INSTALL/LOAD httpfs`, `CREATE OR REPLACE TEMPORARY SECRET
  duct_s3 (TYPE s3, KEY_ID, SECRET, [SESSION_TOKEN], REGION, URL_STYLE, ENDPOINT, USE_SSL)`
  before the ATTACH (`OVERRIDE_DATA_PATH true`, so a lake copied from `./lake` just works), and
  locked mode uses `allowed_directories=['s3://bucket/prefix/']`. SQLite and Postgres catalogs
  both work. With `role_arn` the credentials are STS AssumeRole ones (session policy = lake
  prefix) and a goroutine re-issues the secret before they expire (`CREATE SECRET` stays
  allowed after the lock, ADR 0007). Errors are scrubbed of the keys. Tested end to end in
  `test/integration/s3_rustfs_test.go`.
- **Gotcha:** Arrow strings alias memory owned by the batch; copy (`strings.Clone`) before
  `Release`/next `Next`.
- **Tests:** `engine_test.go` (round trip, WAL header, tx rollback, locked mode, Postgres
  config parsing); Postgres end to end in `test/integration/catalog_postgres_test.go`.
- **Status:** experimental.
