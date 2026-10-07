# 0007. Gobierno

Estado: aceptada (2026-10-03)

## Decisión (KISS, sin servicios nuevos)
- Todo acceso multiusuario pasa por `duct serve`, único proceso con credenciales del catálogo
  y S3.
- DuckDB del servidor bloqueado: `enable_external_access=false`, `allowed_directories` solo al
  lake, `lock_configuration=true`, sin autoinstall/autoload de extensiones.
- Auth: login → rol → vistas del rol. quack-oauth como referencia de diseño.
- Filtros por fila y máscaras = **vistas por rol** generadas desde los grants (sintaxis estilo
  Unity: `GRANT`, `SET ROW FILTER`, `SET MASK`); se verifica que la consulta solo toque el
  schema del rol (parser o plan de DuckDB).
- Código de usuario (marimo, tareas `python`) corre sin credenciales y se conecta por el
  protocolo Postgres o HTTP de `duct serve`. UDF Python en `duct serve` solo para admins.
- Grants, auditoría, lineage y secretos (cifrados con `DUCT_SECRET_KEY`) en tablas de Duct.

## Bloqueo de DuckDB: probado (2026-10-06, DuckDB 1.5.6 + RustFS 1.0.1)
Orden obligatorio, con una conexión privilegiada que adjunta el catálogo antes de bloquear:
```sql
SET allow_persistent_secrets=false;            -- ANTES del primer ATTACH/secret (si no, DuckDB lo rechaza)
-- después de LOAD, CREATE SECRET y ATTACH del lake:
SET allowed_directories=['s3://lake/data/'];  -- antes de bloquear; si no, el lake deja de funcionar
SET enable_external_access=false;
SET autoinstall_known_extensions=false;
SET autoload_known_extensions=false;
SET lock_configuration=true;
```
- Funciona: lecturas y escrituras del lake; se niegan `read_*` fuera del lake, `glob`,
  `COPY TO`, `ATTACH` nativo, `SET`, `INSTALL`/`LOAD`.
- **Huecos:** la extensión sqlite no pasa por el filesystem de DuckDB (`sqlite_scan` y
  `ATTACH … (TYPE sqlite)` leen/crean archivos del host; incluido el catálogo del lake), y
  `CREATE SECRET` sigue permitido. Probablemente igual con la extensión postgres.
- **Por eso el bloqueo no basta: `duct serve` filtra cada consulta con el parser de DuckDB.**
  Solo `SELECT` sobre las vistas del rol; se rechazan `ATTACH`, `DETACH`, `CREATE SECRET`,
  `COPY`, `INSTALL`, `LOAD`, `SET`, `PRAGMA` y las funciones `sqlite_*`, `postgres_*`,
  `read_*`, `glob`, `*_scan`.
- Fuga menor de metadatos: `count(*)` puede responderse desde estadísticas del catálogo sin
  leer S3; irrelevante si el rol solo ve sus vistas.
- Credenciales por rol: STS AssumeRole en RustFS con política de sesión por prefijo
  `lake/data/<schema>/*` funciona (intersección con la política del usuario). Falta probar
  renovación (>15 min).
- Repetir en DuckDB 2.0 cuando haya CLI/driver.

## Restricción por rol en duckgres: probado (2026-10-06)
- **`search_path` no aísla en DuckDB**: con `USE`/`search_path` en el schema del rol, un nombre
  sin calificar igual resuelve a `main`. La barrera real es el autorizador.
- duckgres ya trae `QueryAccessPolicy{ReadOnly, AllowedSchemas, AllowedRelations}` (parsea
  con `pg_query`): niega `main.*`, `read_csv`, `duckdb_tables()`, `ATTACH`, `SET`, DDL; y sus
  metadatos (`pg_class`, `information_schema`) se filtran por rol. Usarlo como filtro de
  sentencias de `duct serve`.
- Exige nombres calificados. Tratar un nombre sin schema como `rol.x` en el autorizador
  **abre un bypass** (DuckDB lo resuelve a `main`). Lo correcto: reescribir el AST
  (`pg_query` + deparse) a `rol.x` solo si existe en el schema del rol, y ejecutar el SQL
  reescrito.
- Ampliar su allowlist de catálogo (`pg_attrdef`, `pg_description`, `pg_am`, `pg_proc`,
  `pg_settings`, `pg_collation`, `key_column_usage`, `table_constraints`) para JDBC y psql.
- Roles restringidos son solo lectura; las escrituras van por otra vía de Duct.

## Gobierno en SQL (v0.2)
Además de `duct role|grant|policy`, el mismo efecto con sentencias estilo Unity Catalog. Parser
propio en `core/governance/sql.go` (errores `línea:columna`); ejecuta sobre el `Store` (cambia las
tablas `duct_*`, regenera las vistas `role_<rol>` y audita).
```sql
CREATE ROLE r;                       DROP ROLE r;
GRANT SELECT ON [TABLE] s.t TO [ROLE] r;     REVOKE SELECT ON [TABLE] s.t FROM [ROLE] r;
GRANT ROLE r TO USER u;              -- fija el (único) rol del usuario; REVOKE ROLE no existe
SHOW GRANTS [ON [TABLE] s.t | TO [ROLE] r];  -- role, privilege, object, row_filter, masks
ALTER TABLE s.t SET ROW FILTER <expr> FOR ROLE r;     ALTER TABLE s.t DROP ROW FILTER FOR ROLE r;
ALTER TABLE s.t ALTER COLUMN c SET MASK <expr> FOR ROLE r;   ... DROP MASK FOR ROLE r;
```
- **Divergencia con Databricks:** allí el filtro/máscara es una UDF SQL aplicada a la tabla
  (`SET ROW FILTER fn ON (cols)`, `SET MASK fn`) para todos. Duct no tiene UDFs SQL en el catálogo
  y sus políticas son por rol: la expresión va en línea y nombra el rol (`FOR ROLE`). La forma
  Databricks se reconoce solo para responder con un error claro. Sin `CREATE USER` en SQL (llevaría
  la contraseña en la sentencia y en la auditoría): usuarios y tokens siguen en el CLI.
- **Entradas:** `duct sql "<sentencia>"` (local; lo que no es gobierno va al motor, como `duct query`)
  y `duct serve` (pg y HTTP).
- **Admin = rol integrado `duct_admin`** (`duct role create duct_admin`, usuarios con ese rol).
  Solo ese rol ejecuta sentencias de gobierno por `duct serve`; es una constante, no una columna,
  y no hay otra forma de volverse admin que `GRANT ROLE duct_admin TO USER u` por un admin o el CLI
  local. El resto de sus consultas siguen sujetas al guard (solo SELECT sobre vistas de su rol).
- **Guard:** `Handler.Administer` reconoce estas sentencias ANTES de las comprobaciones de solo
  lectura y de schema del rol. No admin: `permission denied` (se audita con su texto; el detalle de
  sintaxis no se filtra). Admin: se ejecuta serializado, se audita con el texto original. En pg,
  duckgres no puede ejecutarlas (no son SQL de Postgres): el fork tiene `QueryAccessPolicy.Intercept`,
  que ejecuta la sentencia y devuelve un `SELECT` constante con el resultado. En el protocolo
  extendido se ejecuta en `Parse` (una vez por preparación). Los errores de pg llegan con SQLSTATE
  `42501`, también los de sintaxis.
- **Expresiones de filtro/máscara** (también las de `duct policy`): se validan con
  `json_serialize_sql` de `SELECT (<expr>\n) FROM s.t` y deben ser UNA expresión escalar sobre las
  columnas de esa tabla: sin subconsultas, funciones de tabla, `*`, WITH, WHERE/GROUP/ORDER/LIMIT,
  operaciones de conjuntos ni referencias a otras tablas/schemas; las funciones siguen la misma
  lista negra que las consultas de rol (`read_*`, `glob`, `getenv`, `current_setting`, `sqlite_*`…).
  Después se comprueba que enlaza contra la tabla (columnas inexistentes fallan al crear, no al
  consultar). El `\n` antes del paréntesis de cierre evita que un comentario `--` final se coma la
  vista. Quien necesite un cruce (p. ej. filtrar por una tabla de permisos) lo materializa en la
  tabla base o en un pipeline: no se permite en la expresión.

## Implementado: auditoría (2026-10-07)
Tabla `duct.duct_audit` (una fila por sentencia, permitida o denegada; protocolos pg, http, mcp, web,
admin, sharing, `cli`). Cambios de hoy:
- **Retención**: `[audit] retention = "90d"` (por defecto 90d; `0` = para siempre). La aplican `duct gc`
  (paso `prune audit`), `duct serve` (al arrancar y cada hora) y `duct audit prune --older-than 30d`.
  Borra por lotes de ~5000 filas en orden de tiempo (cada `DELETE` es corto; reintenta si SQLite
  dice `database is locked`). La copia exportada al lake no se poda.
- **Exportación al lake**: `[audit] export = true` agrega las filas nuevas a `lake.bronze.duct_audit_log`
  (solo-agregar) cada minuto en `duct serve`, una vez más al apagar, y con `duct audit export`. Idempotente:
  cada fila lleva `audit_id` (aleatorio; las anteriores a la migración 9 usan un md5 de la fila), la
  marca de agua está en `duct.duct_audit_state` y solo acota el escaneo; se relee 24 h antes de ella
  porque una fila lleva la hora de inicio y se escribe al terminar (una sentencia larga llega "tarde"), y
  lo ya exportado se omite por `audit_id`. Es una tabla bronze normal: ningún rol la lee salvo que un
  admin la conceda.
- **Aciertos de caché** (dashboards): fila con `status=ok`, `cached=true`, `duration_ms` = lo que tardó
  la búsqueda en caché, `rows_returned` = filas del gráfico y SQL `-- semantic query (cache hit) <petición>`.
  Decisión: **se agregan**, no una fila por acierto. Los aciertos con mismo usuario, rol, protocolo y
  petición dentro de 60 s se funden en una fila con `hits = N` (duración promedio). Un dashboard con
  8 gráficos y un espectador que refresca cada 5 s escribe 8 filas por minuto, no 96. Los fallos de
  caché ya se auditan por la vía normal (`cached` nulo/false, `hits` 1).
- **CLI**: `duct query`, `duct sql`, `duct run` y `duct ingest` escriben filas con protocolo `cli`,
  usuario = `$USER` (si falta, el usuario del proceso) y rol `local`; `duct run`/`ingest` registran una
  descripción (`duct run silver.orders`) y no el SQL interno. `[audit] cli = false` lo apaga. El fallo al
  escribir nunca afecta al comando (un proyecto sin migrar, por ejemplo, no deja filas).
- **Tipos e índices**: en Postgres `occurred_at` ya era `TIMESTAMP`. En SQLite no es posible: el escritor
  SQLite de DuckDB declara toda columna de tiempo como `VARCHAR` (probado con TIMESTAMP, TIMESTAMPTZ,
  DATE). La migración 9 reescribe los textos existentes (`10:00:00.5`, `10:00:00`) al formato fijo
  `YYYY-MM-DD HH:MM:SS.ffffff` (UTC), que el escritor usa siempre; así el orden del texto es el orden
  del tiempo, los filtros de rango y el índice son correctos, y `CAST(occurred_at AS TIMESTAMP)` lo lee
  como timestamp en ambos motores. Índices `duct_audit_at (occurred_at)` y
  `duct_audit_user_at (user_name, occurred_at)`: en SQLite con `USE duct; CREATE INDEX`, en Postgres con
  `postgres_execute`; ambos de mejor esfuerzo. También agrega `audit_id`, `cached` y `hits`
  (NULL = falso / 1).
- **Redacción**: antes de guardar, `governance.RedactSQL` oculta el cuerpo de `CREATE SECRET`, asignaciones
  `password=`/`token=`/`api_key=`/`secret_key=…`, cláusulas `PASSWORD 'x'`, `Bearer …`, `user:clave@` en URLs
  y tokens `dct_…`. Es una red de seguridad (prefiere ocultar de más), no un parser; también se aplica
  al leer en `duct audit query`.
- **`duct audit query [--user u] [--since 1h] [--status denied] [--limit 100] [--format table|json]`**:
  filas más nuevas primero; `*` marca los aciertos de caché.
- Evidencia: `cmd/duct/audit_test.go` (CLI, redacción, lotes de poda y retención por `gc`, exportación
  idempotente con filas tardías, fusión de aciertos, migración de datos existentes con índices, bucle de
  mantenimiento) y `TestDashboardCacheBySnapshot` (aciertos auditados).
- Límite conocido: los timestamps de las demás tablas `duct_*` siguen siendo texto en SQLite (mismo motivo).

## Por probar
- Madurez de quack-oauth.

## Después
- ABAC por tags, SSO/OIDC, credential vending a motores externos, OpenLineage.

## Descartados
- Cedar, OpenBao, Unity Catalog OSS, Polaris, Lakekeeper, Gravitino (Iceberg/Delta o
  superficie); RLS de Postgres vía pg_ducklake (huecos de permisos).
