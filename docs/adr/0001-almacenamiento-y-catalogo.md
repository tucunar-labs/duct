# 0001. Almacenamiento y catálogo

Estado: aceptada (2026-10-02)

## Decisión
- Formato de tabla: **DuckLake v1.0**. Es el contrato: cualquier motor futuro lee el mismo lake.
- Catálogo: **SQLite + WAL** por defecto; **Postgres** al pasar a varias máquinas.
- Datos: `storage = ./lake` (directorio con layout de bucket) o `s3://…`. **RustFS**
  recomendado (STS y políticas por prefijo); Garage no permite aislamiento por tabla.
- **MinIO prohibido.**

## Reglas
- Duct no escribe en las tablas del catálogo de DuckLake; todo pasa por DuckLake. Las tablas
  propias de Duct van aparte, con SQL portable entre SQLite y Postgres.
- Rutas relativas al `data_path`: crecer = `rclone sync ./lake` + cambiar `storage`.
- `duct catalog migrate --to postgres://…`: inicializar DuckLake vacío en Postgres y copiar
  tablas con DuckDB (`ATTACH` a ambos), verificar conteos. Sin pgloader.
- SQLite: `journal_mode=WAL`, `busy_timeout` en cada conexión, rechazar carpetas de red o
  sincronizadas, checkpoint en `duct gc`.
- CI corre la misma suite con `./lake` y con `s3://` (Garage y RustFS).

## Notas
- DuckDB no es obligatorio para leer DuckLake: hay clientes propios en DataFusion, Trino y
  Python (`ducklake-dataframe`, que también sirve a Polars). El de Spark solo escribe.

## Implementado: `storage = "s3://bucket/prefix"` (2026-10-07)
- `duct.toml`: `storage = "s3://lake/prod"` y tabla `[s3]` con `endpoint` (host:puerto, sin esquema),
  `region`, `url_style` (`path` por defecto con endpoint propio), `use_ssl`, `role_arn`. **Nunca
  credenciales**: `DUCT_S3_ACCESS_KEY_ID` / `DUCT_S3_SECRET_ACCESS_KEY` / `DUCT_S3_SESSION_TOKEN` o
  `AWS_*`; `DUCT_S3_ENDPOINT|REGION|URL_STYLE|USE_SSL|ROLE_ARN` pisan la tabla.
- `duct init <dir> --storage s3://b/p --s3-endpoint h:9000 --s3-no-ssl [--s3-role-arn ...]`.
- Solo secretos TEMPORARY (los persistentes siguen desactivados). Con `role_arn` el proceso cambia
  su clave por credenciales STS con política de sesión limitada al prefijo (solo lectura en
  `serve-pg`) y las renueva antes de vencer. Verificado en RustFS 1.0.1.
- Migrar de `./lake`: `rclone sync ./lake :s3,endpoint=...:bucket/prefix`, cambiar `storage`
  (y agregar `[s3]`), borrar `./lake`. El catálogo no cambia (`OVERRIDE_DATA_PATH`).
- Prueba: `test/integration/s3_rustfs_test.go` (RustFS en Podman).
