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
