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
- [ ] Primer commit (pendiente de revisión del usuario).
- [ ] Repo en GitHub y CI corriendo (pendiente de autorización para publicar).
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
      rechazadas). Falta: sintaxis SQL (`GRANT …` como sentencia).
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
- Reintento de conflictos lógicos (`Transaction conflict`) en `run` e `ingest` cuando dos
  procesos refrescan la misma tabla.
- Sintaxis SQL para gobierno (`GRANT …`, `SET ROW FILTER`, `SET MASK` como sentencias).
- Subir a DuckDB 2.0; `data_path` remoto en serve-pg; publicar el fork de duckgres.
- Primer commit y repo en GitHub (pendiente del usuario).

## Después de v0.1 (sin orden aún)

Orquestador y triggers · `duct alerts` + canales · contratos ODCS (`odcs-go`) · capa
semántica Ossie + MCP · ECharts · marimo · MLflow opcional · OpenSharing · streaming con
Kafka · Postgres como catálogo multi-máquina · contrato de operabilidad para Duct Cloud.
