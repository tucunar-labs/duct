# Backlog de Duct

Única fuente de estado del proyecto. Las decisiones viven en `docs/adr/`; aquí solo va
qué falta, en orden. Al terminar algo se marca `[x]` con la evidencia (comando o test).

## Objetivo

**Norte:** reemplazo FOSS completo de Databricks, local-first, en un solo binario.

**Meta actual — v0.1 "lake local de punta a punta":** en una máquina limpia, con solo el
binario `duct`:

1. `duct init demo` crea un proyecto con catálogo SQLite y `./lake`.
2. `duct ingest` carga un CSV y una tabla de Postgres (ODBC) a Bronze.
3. `duct run` ejecuta un pipeline SQL Bronze → Silver → Gold con una `EXPECT` que falla y
   otra que descarta filas.
4. `duct serve` permite consultar Gold desde `psql` y desde DBeaver, como un rol con un
   filtro por fila, y no permite ver la tabla base.
5. La web muestra el catálogo y el resultado de una consulta.

Hecho = los cinco pasos corren en `examples/demo/` como test e2e en CI.

## M0 — Fundaciones

- [x] Diseño por capas en `docs/adr/` (0001–0015). Evidencia: 15 ADRs.
- [x] Esqueleto Go: `core/contract/v1` (Arrow), `core/registry`, `cmd/duct`, módulo
      `channel-webhook`. Evidencia: `just check` verde.
- [x] SDK Python `duct-data`. Evidencia: ruff, pyright strict, 12 tests.
- [x] Herramientas con mise, depguard probado, hook de lefthook. Podman rootless + socket.
- [x] Primer commit (`e498071`).
- [x] Repos privados `alonsoburon/duct` y `alonsoburon/duckgres` (rama `duct`); CI verde
      (`check` + `e2e`, con deploy key de solo lectura al fork y Postgres como servicio).
- [ ] Probar los supuestos de riesgo antes de construir encima:
  - [x] `duckdb-go` + DuckLake con catálogo SQLite. Evidencia: e2e en `cmd/duct`.
    Base: **DuckDB 1.5.6**; la línea 2.0 de duckdb-go aún no carga `ducklake` (ADR 0002).
  - [x] Ídem con catálogo Postgres. Evidencia: `test/integration/catalog_postgres_test.go`.
  - [ ] Subir a DuckDB 2.0 cuando duckdb-go publique un tag con extensiones compatibles.
  - [x] DuckLake sobre RustFS con DuckDB bloqueado + STS por prefijo. Evidencia: ADR 0007
    (huecos: extensión sqlite y `CREATE SECRET` → filtro de sentencias obligatorio).
  - [x] duckgres sirve DuckLake; psql/psycopg/JDBC funcionan con fallos acotados. Evidencia:
    ADR 0008 (fork con `replace`, parches listados).
  - [x] marimo 0.25.1 con DuckDB 2.0 dev (Python). Evidencia: ADR 0006 (ojo WebSocket).

## M1 — Lake local

- [x] Módulo `engine-duckdb` + contrato `Engine`: extensiones, `ATTACH` DuckLake, modo
      bloqueado. Evidencia: tests del módulo; `duct query --locked` niega `read_csv('/etc/passwd')`.
- [x] `duct init`: `duct.toml`, catálogo SQLite con WAL y `busy_timeout`, `.gitignore`;
      rechaza NFS/SMB/sshfs y carpetas sincronizadas (`--force`). Evidencia: tests `core/project`.
- [x] `duct query "SQL"` (`--format table|csv|json`, `--locked`). Evidencia: e2e.
- [x] Tablas `duct_*` en `.duct/duct.sqlite` + `duct migrate` idempotente. Evidencia: e2e.
- [x] `duct gc`: flush de inlining, merge, expire, cleanup, huérfanos, checkpoint WAL.
      Evidencia: e2e (datos intactos). Falta: `ducklake_rewrite_data_files` para archivos con deletes.

## M2 — Ingesta

- [x] `Source` CSV/Parquet/JSON (DuckDB nativo) → Bronze, full e incremental, evolución de
      esquema. Evidencia: e2e en `cmd/duct/ingest_test.go`.
- [x] `Source` ODBC (`odbc_scanner`, core) incremental por marca de agua en
      `duct_ingest_state`. Evidencia: `test/integration/odbc_postgres_test.go` (Postgres 17 en
      Podman: 2 → 1 → 0 filas). Ojo: filtro `>` pierde filas con marca igual que llegan tarde.
- [x] `source-bento` embebido para REST (solo `components/pure` e `io`; +30 MB al binario).
      Evidencia: e2e con httptest. Usar `generate` + procesador `http` (el input `http_client`
      no termina).

## M3 — Pipelines

- [x] Parser de `CREATE MATERIALIZED VIEW` / `CREATE STREAMING TABLE` / `CONSTRAINT … EXPECT`.
      Evidencia: golden tests en `core/pipeline/testdata/parse/`.
- [x] Grafo de dependencias con `json_serialize_sql` (ciclos, upstream, `--only`).
- [x] `duct run`: refresco completo en orden, `FAIL UPDATE` conserva versión previa y salta
      downstream, `DROP ROW` filtra y cuenta; historial en `duct_pipeline_runs`. Evidencia:
      `cmd/duct/run_test.go`. Mejora: hoy evalúa el SELECT dos veces (medir y escribir).

## M4 — Serving y gobierno

- [x] `serve-pg` sobre el fork de duckgres con login → rol (TLS opcional, ban por intentos).
      Evidencia: `TestE2EServe` + prueba manual con psql 18.
- [x] Grants, filtros por fila y máscaras → vistas `role_<rol>` (por CLI: `duct grant|policy`);
      guard con el parser de DuckDB. Evidencia: `TestE2EServe` (24 + 33 sentencias hostiles
      rechazadas). Sintaxis SQL: ver v0.2.
- [x] Auditoría en `duct.duct_audit` (permitidas y denegadas, ambos protocolos).
- [x] `serve-http` estilo ClickHouse (JSON, CSV, ArrowStream, Parquet) + `/healthz` y `/metrics`.

## M5 — Web mínima

- [x] `modules/web`: templ + Datastar + Tailwind standalone + CodeMirror (esbuild vía Go),
      embebido, sin Node. Evidencia: 16 tests de `modules/web`.
- [x] Login, catálogo (solo lo del rol) y editor SQL con resultados. Evidencia: prueba manual
      2026-10-06 contra `duct serve` real.

## Cierre de v0.1

- [x] `examples/demo/` con los cinco pasos como test e2e (`just e2e`, job `e2e` en CI).
      Evidencia: `TestDemo` PASS (15 s), con ODBC contra Postgres en Podman.
- [x] Catálogo Postgres: catálogo DuckLake + estado de Duct en Postgres, contraseña por env,
      `duct catalog migrate|info`, 12 escritores concurrentes. Evidencia: `just test-integration`.
- [x] Web: sesiones en cookie cifrada (XChaCha20-Poly1305, rotación de clave, timeouts) y
      límite de intentos por usuario e IP (429). Evidencia: `modules/web/session_test.go`.
- [x] `duct run` evalúa el SELECT una vez (staging en memoria). Evidencia:
      `TestRefreshEvaluatesOnce` (20 000 filas aleatorias, conteos exactos).

**v0.1 cumplida (2026-10-07):** `just check`, `just e2e` y `just test-integration` en verde.

Pendientes conocidos para v0.2:
- [x] Reintento de conflictos (`core/txretry`: conflictos de DuckLake, serialización de
  Postgres y SQLite ocupado) en `run` e `ingest`; lease por fuente en la ingesta (sin duplicados).
  Evidencia: `TestConcurrentRunSameTarget`, `TestConcurrentIncrementalIngest`, `TestIngestLock`
  (10/10 tras reintentar "database is locked").
- [x] Sintaxis SQL para gobierno (`CREATE ROLE`, `GRANT`, `SHOW GRANTS`, `ALTER TABLE … SET ROW
  FILTER | ALTER COLUMN … SET MASK … FOR ROLE`): `duct sql` y, por `duct serve`, solo el rol
  `duct_admin`. Evidencia: golden tests en `core/governance/testdata/sql`, `TestE2EGovernanceSQL`,
  `TestE2EServeAdmin`. Ver ADR 0007. Parche `QueryAccessPolicy.Intercept` commiteado en el fork (`3e26bf58`).
- [ ] Subir a DuckDB 2.0 (esperar tag de duckdb-go con extensiones compatibles).

## v0.2 — Almacenamiento S3

- [x] `storage = "s3://bucket/prefix"` de punta a punta con RustFS (SQLite y Postgres como
      catálogo): init → ingest → run → gc → serve (pg + http) como rol; bloqueo con
      `allowed_directories` al prefijo (niega otro prefijo del mismo bucket); credenciales solo
      por env, secretos TEMPORARY. Evidencia: `test/integration/s3_rustfs_test.go`.
- [x] STS AssumeRole con política de sesión por prefijo (`[s3] role_arn`), renovación antes de
      vencer (test con STS falso; en RustFS real falta esperar un vencimiento). Evidencia:
      `TestS3STSScopedCredentials`, `core/s3conn`.
- [x] Migración `./lake` → `s3://` con `rclone sync` + cambiar `storage`. Evidencia:
      `TestS3MigrateFromLocal`.
- [x] Job de CI con `test-integration`: ver «Pendiente»: job `integration`.

## Streaming y alertas (2026-10-07)

- [x] `duct stream`: Kafka → Bronze (franz-go), commit de offsets tras el commit de DuckLake,
      sin duplicados por reentrega, rejects, evolución de esquema, dentro de `duct serve` con
      `[stream]`. Evidencia: `TestStreamWriter` (consumidor falso), `TestStreamKafkaToBronze`
      (Kafka real en Podman: mediana 205 ms, `kill -9` sin pérdida ni duplicados, lag 0).
- [x] `duct alerts`: reglas `alerts/*.sql`, ventana en DuckDB en memoria, estado firing/resolved
      con dedup, registro en `bronze.duct_alerts`; canales ntfy, webhook, PagerDuty, WhatsApp.
      Evidencia: `core/alerts` tests, `TestAlertsRunner`, `TestAlertsKafka` (umbral ~115 ms,
      resolución ~195 ms, sensor mudo a 2,0 s), `TestServeRunsStreamAndAlerts`.
- [ ] WhatsApp contra la API real de Meta (hoy solo `httptest`: falta cuenta, número y plantilla
      *utility* aprobada).
- [x] Alertas (2026-10-07, ADR 0009): `FOR`/`RESOLVE_FOR`/`REPEAT`, silencios (`duct alerts silence`)
      y activo/activo con un lease por (regla, clave) en el estado de Duct. Evidencia:
      `TestTracker*`, `TestAlertsHysteresisAndReminders`, `TestAlertsSilences`, `TestAlertsActiveActive`
      (SQLite, dos instancias) y `test/integration` `TestAlertsActiveActive` (Postgres + Kafka, `kill -9`).
- [ ] Streaming: Avro/Protobuf con registry, métricas, DLQ por tipo incompatible.

## v0.3 — Orquestación, contratos, semántica (2026-10-07)

- [x] Orquestador: `jobs/*.yaml` (cron con zona horaria, `on_snapshot`, `on_file`, DAG con
      reintentos/timeout), tareas pipeline/ingest/shell/python/notebook, alertas por canal;
      una corrida por tick entre varias máquinas. Evidencia: `TestE2EJobs*`,
      `TestJobsTwoServersFireOnce`.
- [x] Contratos ODCS (`duct contract lint|diff|test|compile|freshness`), aplicados en `duct run`
      y en jobs vía `contracts.RunPipeline`. Evidencia: `TestE2EContracts`, demo con contrato.
- [x] Capa semántica Ossie 0.2.0-dev (compilador propio sobre vistas por rol, sin fan-out) +
      servidor MCP (`list_metrics`, `describe`, `query_metrics`). Evidencia: `TestE2EMCP`.
- [x] Streaming y alertas (sección anterior).

## Dashboards (2026-10-07)

- [x] `dashboards/*.yaml` (filtros select/multiselect/daterange, gráficos line|bar|area|pie|table|kpi|
      scatter|heatmap, layout en grilla de 12, opciones acotadas) + `specs/dashboard.schema.json` +
      `duct dashboard lint|list` (valida contra el modelo Ossie: nombres y joins sin fan-out).
      Evidencia: `core/dashboards` tests, `TestDashboardLint`.
- [x] Web: `/dashboards`, `/dashboards/{name}`, `POST .../data` (SSE), consulta por `SemanticQuerier`
      como la identidad de la sesión, caché LRU por (snapshot DuckLake, rol, request), filtros
      cruzados por click/brush con Datastar, isla ECharts 6.1.0 (`static/charts.js`, 670 KB) con temas
      claro/oscuro. Evidencia: `modules/web/dashboards_test.go`, `TestE2EDashboards`, capturas en
      `/tmp/duct-ui-shots/dashboard-*.png` (Chromium headless, sin errores de consola/CSP).
- [ ] Dashboards: caché compartida entre procesos (hoy en memoria), exportar a imagen/PDF, dashboards
      por rol (`grants` sobre el YAML), parámetros en la URL (filtros compartibles), alertas sobre un gráfico.

## Pendiente (sin orden aún)

- [x] marimo integrado: `duct notebook` (servidor privado + token del usuario, entorno con lista blanca)
      y montaje ASGI con auth HTTP+WebSocket (protocolo `notebook`, `[serve.notebook]`). Evidencia:
      `TestNotebookScript`, `TestServeNotebookAuth` (WebSocket sin token: 403), `test_asgi.py`, `TestSDKTransports`
      (`duct.connect` por ArrowStream y psycopg). Falta: modo edición en el montaje, comentarios e historial.
- [x] MLflow opcional (`duct enable mlflow`, `duct mlflow`, `mlflow.*` por `ATTACH` de solo lectura,
      `duct.mlflow.score`). Evidencia: `TestMLflowIntegration` (MLflow 3.17.0 real), `TestMLflowOnPostgresCatalog`
      (`just test-integration`). Falta: copiar métricas a Bronze, serving online.
- [x] UDFs Python (`functions/*.py`, `@duct.function`, escalares y vectorizadas) y ejecutables stdin/stdout
      (`functions/*.toml`); solo `duct_admin` en `duct serve`. Evidencia: `TestE2EFunctions`,
      `TestE2EServeFunctionsAdminOnly`, `core/udf` tests; costo por lote en ADR 0005. Falta: tipos
      no escalares (DATE, DECIMAL, LIST), funciones de tabla/agregado, recarga en caliente.
- [x] OpenSharing fase 1 (por consulta) y fase 2 (servidor REST con copias Parquet filtradas y
      enmascaradas, URLs firmadas ~15 min, local y S3) (ADR 0014, sección Implementado).
      Evidencia: `TestE2EShare` (cliente oficial `delta-sharing`), `TestShareS3RustFS`,
      `modules/sharing-server` tests. Falta: CDF, `predicateHints`, Iceberg REST, límite de intentos.
- [x] Contrato de operabilidad (ADR 0013, 2026-10-07): `duct backup|restore` con manifiesto y
      verificación (catálogo SQLite o Postgres, lake local o S3, `--catalog-only`, `--keep`), API admin
      `/admin/v1` (+ `specs/admin.openapi.yaml`), cuotas por rol, `/readyz` y `/metrics` ampliado.
      Evidencia: `TestE2EBackupRestoreSQLite`, `TestE2EAdminAPI`, `TestQuotaConcurrent`,
      `TestBackupRestoreS3` (SQLite y Postgres sobre RustFS).
- [x] Auditoría v2 (2026-10-07, ADR 0007): retención `[audit] retention` (gc, serve cada hora,
      `duct audit prune`), exportación idempotente a `bronze.duct_audit_log` (`[audit] export`,
      `duct audit export`), aciertos de caché de dashboards (agregados por minuto), filas del CLI
      (`[audit] cli`), `occurred_at` normalizado + índices, redacción y `duct audit query`. Evidencia:
      `cmd/duct/audit_test.go`. Falta: columnas de tiempo reales en SQLite (límite de DuckDB), redacción
      con parser en vez de expresiones regulares.
- [ ] Cuotas en el protocolo Postgres (usa su propia sesión DuckDB: hoy solo HTTP, MCP y web).
- [ ] Respaldo: cifrado (lakes con `encrypted`), objetos de más de 5 GiB, restauración parcial por tabla.
- [x] Job de CI `integration` (docker run de Postgres, RustFS y Kafka + `just test-integration` con
      `DUCT_TEST_*`). Evidencia: las 17 pruebas pasan en local contra servidores externos (192 s);
      falta verlo verde en Actions.
- [x] Semántica (2026-10-07, ADR 0011): métricas derivadas entre hechos, filtros sobre métricas (HAVING),
      preagregados por rol en `semantic_cache` (solo si frescos; una política nueva los invalida al
      instante) y recarga en caliente del modelo. Evidencia: `core/semantic` (goldens),
      `TestSemanticDerivedHavingAndPreaggregates`, `TestServeSemanticHotReload`.
- [x] Jobs (2026-10-07, ADR 0005): recarga en caliente de `jobs/`, `[jobs] history_retention`, `catchup`.
      Evidencia: `TestJobsHotReload`, `TestJobsCatchup`, `TestJobsHistoryRetention`.
- [ ] Semántica: preagregados incrementales (hoy reconstrucción completa por rol), dashboards YAML con
      filtros de métrica, `refresh` por horario.
- [ ] Alertas: persistir los relojes de `FOR`/`RESOLVE_FOR`; silencios por etiqueta/severidad.
- [ ] Subir a DuckDB 2.0 (despriorizado por el usuario).
