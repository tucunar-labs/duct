# test/integration/

Integration tests behind the `integration` build tag (testcontainers: Postgres, Kafka,
RustFS). Run with `just test-integration`. Today: `odbc_postgres_test.go` (Postgres in Podman via psqlODBC, needs unixODBC + psqlodbc; skips otherwise).
`catalog_postgres_test.go` drives the built `duct` binary against Postgres 17 in Podman:
Postgres catalog + Duct state (init, ingest, run, serve), `duct catalog migrate` from SQLite,
concurrent writers. Needs `podman` (rootless) and the `postgres` DuckDB extension (network the
first time).
