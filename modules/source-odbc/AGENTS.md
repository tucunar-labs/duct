# source-odbc

- **Purpose:** pull from any ODBC source (Oracle, HANA, DB2, SQL Server, Postgres...) into
  `bronze.<table>`, full or incremental by watermark (ADR 0003).
- **Contract:** `core/contract/v1` `KindSource` + optional `SQLSource`; registers as
  `source/odbc`. It returns `SELECT * FROM odbc_query('<conn>', '<remote sql>')`; the core
  runs it and keeps the watermark in `duct.duct_ingest_state`, saved only if the load commits.
- **Extension:** `odbc_scanner` is a **core** extension (not community) for DuckDB 1.5.6:
  `INSTALL odbc_scanner; LOAD odbc_scanner` (downloads from extensions.duckdb.org the first
  time, cached in `~/.duckdb/extensions/`). Functions: `odbc_query(conn, sql [, params])`,
  `odbc_connect`, `odbc_close`, `odbc_copy`, `odbc_list_drivers`, `odbc_list_data_sources`, ...
  It needs the system library `libodbc.so.2` (unixODBC) and the vendor driver; neither ships
  with Duct. Arch: `unixodbc` + AUR `psqlodbc`; Debian: `unixodbc odbc-postgresql`.
  The extension is loaded by the core as a plan `Setup` step, so the engine must run
  unlocked (ingest does).
- **Options:** `connection` (required; `DSN=name` or `Driver={...};Server=...`; `${VAR}`
  references are expanded from the environment so passwords stay out of git),
  `table` (`[catalog.][schema.]name`) or `query` (remote SQL, not both).
- **Incremental:** `mode: incremental`, `watermark: col` -> the remote query becomes
  `SELECT * FROM (<query>) duct_src WHERE col > <last>`, so filtering happens in the source
  database. Bare literal when the last value looks numeric, quoted otherwise (ISO timestamps).
  Rows with a watermark equal to the last value but arriving later are missed (`>` not `>=`).
- **Delivery:** at-least-once. If the data commits but the state write fails, the next run
  repeats rows (the CLI says so).
- **Tests:** `odbc_test.go` (plan SQL); `test/integration/odbc_postgres_test.go`
  (`-tags integration`, Postgres in Podman via psqlODBC; skips without unixODBC/driver).
- **Status:** experimental.
