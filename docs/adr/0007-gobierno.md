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

## Por probar
- Madurez de quack-oauth.

## Después
- ABAC por tags, SSO/OIDC, credential vending a motores externos, OpenLineage.

## Descartados
- Cedar, OpenBao, Unity Catalog OSS, Polaris, Lakekeeper, Gravitino (Iceberg/Delta o
  superficie); RLS de Postgres vía pg_ducklake (huecos de permisos).
