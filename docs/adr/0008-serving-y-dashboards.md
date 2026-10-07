# 0008. Serving y dashboards

Estado: aceptada (serving 2026-10-03; dashboards 2026-10-04)

## `duct serve`: un proceso, varios protocolos
- Base: **fork de duckgres** (PostHog, Go, MIT): protocolo Postgres + emulación de
  `pg_catalog`, soporta DuckLake. Una sola auth, vistas por rol y DuckDB bloqueado.
- Protocolo Postgres: ODBC/JDBC/Power BI (conector nativo; Import por defecto)/Excel/Tableau.
- HTTP con el contrato de la API HTTP de ClickHouse (POST SQL,
  `FORMAT JSON/CSV/Parquet/ArrowStream`): vía columnar.
- Después: Flight SQL con `arrow-go/flightsql`.
- Por probar: Power BI (Import y DirectQuery) y ODBC contra duckgres.

## duckgres: probado (2026-10-06, commit 551a974, DuckLake con catálogo SQLite)
- Sirve DuckLake con SQLite (`metadata_store: "sqlite:…"`, no documentado); psql, psycopg y
  pgjdbc funcionan; casi toda la introspección de `pg_catalog`/`information_schema` y JDBC
  `DatabaseMetaData` pasa.
- Fallan: `\d`/`\d+` de psql 18 (`pg_partition_ancestors`, UNNEST), JDBC `getImportedKeys`
  (`MaxIndexKeys`), `pg_get_viewdef` vacío, `current_user` devuelve `duckdb`; query log
  solo con Postgres (Duct necesita su propia auditoría).
- Integración: **fork con `replace`**. `server/` es importable vía la API exportada de su
  control plane (`server/exports.go`): accept loop propio (~130 líneas) con auth de Duct +
  `RunMessageLoop` de duckgres. No hay hook de auth/handler en `server.New`.
- Peso: importar `server/` trae ~579 paquetes (AWS SDK sí, k8s no), binario ~52 MB, build
  frío ~67 s. Podar con build tags en el fork.
- **Fork local listo** (2026-10-06): `~/code/duckgres`, rama `duct`, 7 commits sobre upstream
  `551a974`: allowlist de catálogo, nombres sin schema resueltos al schema del rol (bypass
  cerrado y testeado), `current_user` real, `\d`/`\d+` de psql 18 y `getImportedKeys` de
  pgjdbc, `Config.QueryAuditHook` por sentencia (funciona con SQLite), y test que impide que
  `server/` arrastre k8s/trino (578 paquetes, 0 de k8s). En Duct:
  `replace github.com/posthog/duckgres => ../duckgres` y por sesión `NewLocalExecutor` →
  `QueryAccessPolicy{ReadOnly, AllowedSchemas, DefaultSchema, RelationExists}` →
  `InitSessionDatabaseMetadataWithAccess` → `NewClientConn` + `SetQueryAccessPolicy` +
  `RunMessageLoop`. Pendiente: detalles cosméticos de `\d+`; publicar el fork.
- Parches previstos en el fork (hechos salvo publicar): autorizador + allowlist de catálogo, reescritura de nombres
  sin schema, query log en SQLite o hook de auditoría, `pg_partition_ancestors`,
  `MaxIndexKeys`, `current_user`.
- Plan B en Rust: `pgwire` + DuckDB + axum + arrow-flight.

## Filtros rápidos sin ClickHouse
- DuckLake con sorted tables y particiones alineadas a los filtros + caché local NVMe.
- Caché de resultados invalidada por snapshot de DuckLake; opcional: copias `.duckdb` de las
  tablas Gold más usadas.
- ClickHouse solo si aparece analítica de cara al cliente con cientos de usuarios
  simultáneos, como serving derivado (referencia: fork de fuziontech que lee/escribe DuckLake).

## Dashboards
- **Apache ECharts** para dibujar + **capa semántica** (ADR 0011) para consultar: cada
  gráfico llama a `query_metrics(dims, measures, filters)`, la misma herramienta que Genie.
- Filtros cruzados con eventos de ECharts (click/brush → filtro → reconsulta).
- Velocidad: vistas preagregadas desde la capa semántica + caché por snapshot + lake ordenado.
- Dashboards como YAML en git. Alertas sobre consultas SQL: tabla `alerts` del orquestador.

## Descartados
- GizmoSQL (permisos de pago), BoilStream (sin licencia), myduckserver (sin auth),
  riffq/pgwire/ROAPI/Spice, ODBC de DuckDB para Power BI. pg_ducklake como plan B.
- Mosaic como capa por defecto: 0.x inestable y sus preagregados `mosaic.preagg_<hash>`
  filtran datos entre roles. Si se usa para exploración: schema por rol (`mosaic_<rol>`).
- ClickHouse como serving por defecto; híbrido MergeTree caliente + aging a DuckLake.
