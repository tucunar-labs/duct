# 0002. Cómputo y lenguajes

Estado: aceptada (2026-10-02; lenguajes 2026-10-03)

## Decisión
- Cómputo: **solo DuckDB 2.0**. Se desarrolla sobre el alpha (sep-2026) y se pasa a estable
  al salir (~2.ª mitad de oct-2026). Un solo dialecto SQL.
- **Go** para el núcleo (CLI `duct`, `duct serve`, orquestador, pipelines, ingesta, alertas,
  UI) en un solo binario, con `duckdb-go` (cgo).
- **Python** solo como SDK de código de usuario (UDFs, `@duct.table`, tareas `python`,
  marimo), en procesos aparte que hablan con `duct serve`.

## Estado de la versión (2026-10-06)
- **Base actual: DuckDB 1.5.6** (`duckdb-go v2.10506.0`). La línea 2.0 de duckdb-go
  (`v2.20000.0-6.preview`) aún no carga `ducklake`, y las extensiones del alpha solo sirven
  para su build exacto. Subir `duckdb-go` + `duckdb-go-bindings` juntos cuando haya un tag
  compatible; no se prevén cambios de código.
- Resultados en Arrow requieren el build tag `duckdb_arrow` (exportado en el `justfile`).
- En Python (marimo) el wheel `2.0.0.dev` sí funciona con DuckLake.

## Nada distribuido todavía
- Ni en código ni en interfaz hasta que un cliente lo pida (no antes de ~oct 2027).
- Candidatos entonces: reparto de scans/agregados vía Quack; Ballista + datafusion-ducklake
  para joins gigantes, solo en consultas, nunca en pipelines.

## Descartados
- Spark + Iceberg + StarRocks como núcleo (saca DuckLake; local-first lento).
- ClickHouse como motor o almacenamiento principal (no lee DuckLake; MergeTree no es formato
  abierto). Sail: vigilar cuando tenga streaming y pipelines declarativos.
- Rust para el núcleo (duckgres y Bento son Go).
