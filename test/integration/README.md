# test/integration/

Integration tests behind the `integration` build tag (testcontainers: Postgres, Kafka,
RustFS). Run with `just test-integration`. Today: `odbc_postgres_test.go` (Postgres in Podman via psqlODBC, needs unixODBC + psqlodbc; skips otherwise).
`catalog_postgres_test.go` drives the built `duct` binary against Postgres 17 in Podman:
Postgres catalog + Duct state (init, ingest, run, serve), `duct catalog migrate` from SQLite,
concurrent writers. Needs `podman` (rootless) and the `postgres` DuckDB extension (network the
first time).
`s3_rustfs_test.go` runs RustFS (`docker.io/rustfs/rustfs`, bound to 127.0.0.1, removed at the end)
and drives the binary with `storage = "s3://bucket/prefix"`: init -> ingest -> run -> gc -> serve
(pg + http) as a role, locked DuckDB denying another prefix of the same bucket, STS AssumeRole
scoped to the prefix, and the `./lake` -> `s3://` move. For the CI job: Podman is enough (first run
also downloads the `httpfs` extension); with an S3 service container set `DUCT_TEST_S3_ENDPOINT`,
`DUCT_TEST_S3_ACCESS_KEY`, `DUCT_TEST_S3_SECRET_KEY` (the STS test needs a server with AssumeRole,
i.e. RustFS) and `DUCT_TEST_RUSTFS_IMAGE` pins the image.
`stream_kafka_test.go` runs Kafka (`docker.io/apache/kafka`, KRaft, one node, bound to 127.0.0.1,
removed at the end; `DUCT_TEST_KAFKA_IMAGE` pins the image, `DUCT_TEST_KAFKA_BROKER` uses an
existing broker) and drives the binary: `duct stream` (produce -> Bronze latency, `kill -9`
without loss or duplicates, rejects, lag 0), `duct alerts` (threshold and silent-sensor rules,
resolution, ntfy/webhook/PagerDuty/WhatsApp against `httptest`, Bronze record) and `duct serve`
with `[stream]` + `[alerts]`. `DUCT_TEST_KEEP_FAILED=<dir>` copies the project of a failed test there.

## Servidores externos (CI) y Podman (local)

Cada helper usa un servidor externo si su variable está puesta, y si no lo levanta con Podman
(y lo borra al terminar):

| Servidor | Variables | Podman (por defecto) |
|---|---|---|
| Postgres 17 | `DUCT_TEST_PG_DSN` = conexión de superusuario, p. ej. `postgres://postgres:pw-catalog@127.0.0.1:5432/postgres` (la contraseña se toma de ahí) | `postgres:17-bookworm`, `max_connections=300` |
| S3 (RustFS) | `DUCT_TEST_S3_ENDPOINT` (`host:puerto`), `DUCT_TEST_S3_ACCESS_KEY`, `DUCT_TEST_S3_SECRET_KEY` (la prueba de STS pide un servidor con AssumeRole, o sea RustFS) | `rustfs/rustfs` (`DUCT_TEST_RUSTFS_IMAGE`) |
| Kafka (KRaft) | `DUCT_TEST_KAFKA_BROKERS` (o `DUCT_TEST_KAFKA_BROKER`) | `apache/kafka` (`DUCT_TEST_KAFKA_IMAGE`) |

Con un Postgres externo, `createDB` borra y recrea la base de nombre fijo (`lake1`, `jobs`, ...)
para que las pruebas no se pisen entre ellas; no apuntes `DUCT_TEST_PG_DSN` a un servidor con datos.
Ninguna prueba necesita controlar un contenedor (los `kill -9` van al proceso `duct`, nunca al
servidor; el lag de Kafka se lee por el protocolo), así que **en CI no se salta ninguna** salvo la
de ODBC si faltan `unixodbc`/`odbc-postgresql` (el job los instala con apt).

`backup_test.go`: `duct backup` / `duct restore` sobre RustFS con catálogo SQLite y Postgres:
copia del lado del servidor dentro del bucket, copia a carpeta local, `--catalog-only`, restauración
a otro catálogo y otro prefijo, y rechazo de un catalog-only sin sus datos.

