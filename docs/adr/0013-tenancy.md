# 0013. Tenancy

Estado: aceptada (2026-10-04)

## Decisión: modelo silo
- El FOSS es de un solo tenant (sin `workspace_id`).
- Duct Cloud corre el mismo binario por cliente: su `duct serve`, su base de catálogo, su bucket.
- Infra compartida solo donde es seguro: clúster Postgres con una base por cliente, RustFS
  con un bucket por cliente.
- Plano de control aparte (provisión, versiones, monitoreo, respaldos, facturación).
- Código de usuario en sandbox (Podman/gVisor/Firecracker).

## Duct Cloud como repo aparte (2026-10-06)
- Duct Cloud vive en **otro repo** y consume Duct como artefacto publicado (imagen OCI o
  binario), nunca como fork del código. Licencia de Cloud libre de elegir.
- Duct expone un **contrato de operabilidad** genérico y estable:
  - `/healthz` y `/metrics` (Prometheus);
  - configuración por variables de entorno o archivo, sin estado escondido;
  - `duct migrate` versionado (catálogo y tablas propias);
  - `duct backup` y `duct restore`;
  - API admin: usuarios, roles, tokens, cuotas.
- Regla: nada en el FOSS que solo tenga sentido para Cloud. Lo que Cloud necesite entra al
  FOSS como función genérica de operación.

## Implementado: contrato de operabilidad (2026-10-07)

Todo es genérico y vive en el FOSS; nada es solo de Cloud.

**Sondas y métricas** (módulo `serve-http`, sin autenticación, escuchan donde `[serve.http]`):
- `/healthz`: liveness, no revisa nada. `/readyz`: el catálogo responde (`duct_migrations` y
  `ducklake_snapshots`) y el almacenamiento acepta una escritura (archivo temporal en `lake/`, o un
  objeto `.duct-ready-*` en el prefijo S3); 200 o 503 con el detalle sin credenciales; resultado en
  caché 5 s.
- `/metrics`: texto Prometheus 0.0.4 desde un registro propio sin dependencias (`core/metrics`;
  contadores, gauges, histogramas con etiquetas y colectores). Series: `duct_queries_total{protocol,status}`
  (ok, error, denied, quota; todos los protocolos, incluido `admin`), `duct_query_duration_seconds`,
  `duct_rows_returned_total`, `duct_auth_failures_total{scheme}`, `duct_quota_rejections_total{kind}`,
  `duct_ingest_runs_total` / `duct_ingest_rows_total{source}`, `duct_stream_rows_total` /
  `_rejects_total` / `_batches_total` / `duct_stream_lag_seconds` (edad del mensaje más viejo del
  último lote) / `duct_stream_last_commit_timestamp_seconds`, `duct_alert_transitions_total`, más las
  `duct_http_*` de siempre. Leídas de las tablas de estado en cada scrape (máx. una vez por 10 s), así
  cuentan lo hecho por cualquier proceso (un `duct run` en cron, otro servidor): `duct_pipeline_runs_total{status}`,
  `duct_job_runs_total{status}`, `duct_task_runs_total{type,status}`, `duct_alerts_firing{rule,severity}`,
  `duct_lake_snapshot_id`, `duct_migrations_version`, `duct_build_info{version}`.

**Configuración sin estado escondido**: `duct.toml` y variables de entorno (`DUCT_CATALOG_URL` /
`DUCT_CATALOG_PASSWORD`, `DUCT_S3_*` / `AWS_*`, `DUCT_KAFKA_*`, `DUCT_WEB_SESSION_KEY`...). El estado
está en el catálogo, las tablas `duct_*` y el lake; el único archivo suelto es `.duct/web_session.key`
(clave de sesiones web, se regenera; no se respalda).

**`duct migrate`**: ya era versionado e idempotente (`duct_migrations`); `/admin/v1/status` informa
aplicadas, última y pendientes. Migración 8 = `duct_quotas`.

**`duct backup --to <carpeta|s3://bucket/prefix> [--catalog-only] [--keep N]`**, `duct backup list --to X`
y **`duct restore --from <respaldo|raíz> [--id ID] [--catalog postgres://…] [--storage …] <carpeta vacía>`**.
Cada respaldo es una subcarpeta `<UTC>/` (`20261007T153045Z`):
```
manifest.json            al final: sin él el respaldo está incompleto y se ignora
catalog/<tabla>.parquet  cada tabla ducklake_* (con las de datos inlined)
state/<tabla>.parquet    cada tabla duct_* (usuarios, grants, auditoría, jobs, shares, cuotas...)
project/<ruta>           duct.toml y archivos del proyecto (nunca .env*, .duct/, lake/, .git/)
data/<ruta>              archivos de datos y de deletes que el catálogo referencia
```
- Consistencia: el catálogo se lee en una sola transacción por base (snapshot de lectura en SQLite,
  REPEATABLE READ en Postgres) a Parquet con DuckDB, así que sirve igual para ambos catálogos y no
  necesita `pg_dump`. El `snapshot_id` del manifiesto sale de esa misma copia; los conteos por tabla
  se calculan con `AT (VERSION => id)`. Los escritores pueden seguir; `duct gc` no debe correr a la
  vez (el respaldo falla en vez de quedar mal). Las tablas `duct_*` van en otra transacción.
- Archivos de datos: todos los que el catálogo aún referencia (conserva el viaje en el tiempo). Local →
  copia; S3 → S3 en el mismo servidor, copia del lado del servidor (sin pasar bytes por `duct`; hasta
  5 GiB por objeto, sin sha256 en el manifiesto); otro destino, en streaming con sha256 y tamaño.
  `--catalog-only` (solo con lake en S3) omite los datos; la restauración comprueba que estén.
- Manifiesto: formato, id, fecha, versiones (duct, DuckDB, DuckLake), tipo de catálogo, almacenamiento,
  `snapshot_id`/hora, migraciones, tablas con filas, tablas de catálogo y de estado, archivos con tamaño
  y sha256, y los archivos del lake referenciados. Sin secretos; sí hay hashes de contraseñas y tokens
  en `state/`: tratar el respaldo como el catálogo.
- `--keep N` borra los respaldos completos más viejos tras uno bueno (y carpetas incompletas de más de
  1 día). Un destino S3 usa la misma conexión que el lake (`[s3]` + `DUCT_S3_*`).
- Restauración: carpeta vacía; escribe `duct.toml` del respaldo (con `--catalog`/`--storage` cambiados;
  la contraseña nunca se escribe), inicializa un DuckLake vacío, copia los datos (omite lo que ya está
  con el mismo tamaño), reemplaza catálogo y tablas `duct_*` (conserva `data_path` y la versión del
  destino), aplica las migraciones que falten y reabre el proyecto para **verificar**: `snapshot_id`,
  tablas y filas de cada tabla, tablas de estado, y que cada archivo del lake exista con su tamaño.
  Los sha256 se comprueban al copiar. Rechaza un lake destino con tablas o historial. Funciona entre
  catálogos y almacenamientos distintos del original.

**API admin** (`core/admin`, montada por `serve-http` en `/admin/v1`; OpenAPI en
`specs/admin.openapi.yaml`): usuarios (crear con o sin contraseña, deshabilitar/habilitar,
`password-reset` que devuelve una contraseña temporal una sola vez, cambiar rol, borrar), roles,
grants, filtros por fila y máscaras, tokens (crear devuelve el secreto una vez; listar nunca lo da;
revocar), cuotas y `/status` (versión, DuckDB, catálogo sin secretos, almacenamiento, migraciones,
último snapshot, salud de jobs, stream y alertas). Solo el rol `duct_admin` (401 sin credenciales, 403 a
otro rol), mismos hashes y bloqueos que el resto; cada llamada, permitida o no, queda en `duct_audit`
con protocolo `admin` y sin secretos. Un admin no puede borrarse ni deshabilitarse a sí mismo.

**Cuotas** (por rol, `duct_quotas`): `max_concurrent` (sentencias a la vez en el proceso) y `max_rows`
(filas por sentencia: se corta antes del lote que la cruzaría). Las aplica `governance.Handler`, o sea
HTTP, MCP y web, con 429. El protocolo Postgres (sesión DuckDB propia) las aplica envolviendo el
ejecutor de duckgres (`modules/serve-pg/quota.go`) con `contract.Limiter` del mismo handler: mismo
contador de concurrencia y misma métrica `duct_quota_rejections_total`. Error con SQLSTATE **53400**
(`configuration_limit_exceeded`); el parche del fork (`embedderSQLState`) deja pasar ese código. `max_rows`
corta el flujo al fila `max_rows+1` (el cliente ya pudo recibir las anteriores y recibe el error al final).
Las consultas de catálogo de las herramientas (`\d`, JDBC) también cuentan. Cambios por la API valen al instante; por otro proceso, en ≤5 s.

**CI** (`integration` en `.github/workflows/ci.yml`): `docker run` de Postgres 17, RustFS y Kafka KRaft
en el runner y `just test-integration` con `DUCT_TEST_PG_DSN`, `DUCT_TEST_S3_*` y `DUCT_TEST_KAFKA_BROKERS`;
los helpers de `test/integration` usan esos servidores si las variables existen y Podman si no. No se salta
ninguna prueba por falta de control de contenedores.

Evidencia: `TestE2EBackupRestoreSQLite`, `TestE2EAdminAPI`, `TestQuotaConcurrent`, `core/metrics` y
`core/backup` (unitarias), `TestBackupRestoreS3` (SQLite y Postgres sobre RustFS) y las 17 pruebas de
`test/integration` contra servidores externos (192 s).
