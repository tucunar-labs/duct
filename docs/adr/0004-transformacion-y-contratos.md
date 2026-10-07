# 0004. Transformación y contratos de datos

Estado: aceptada (transformación 2026-10-03; contratos 2026-10-06)

## Pipelines
- **Pipelines declarativos propios, al estilo Lakeflow Declarative Pipelines**, sobre DuckDB +
  DuckLake. SQL estándar, sin Jinja ni YAML: `CREATE MATERIALIZED VIEW`,
  `CREATE STREAMING TABLE`, `CONSTRAINT … EXPECT … ON VIOLATION DROP ROW | FAIL UPDATE`,
  decorador `@duct.table`.
- Dependencias y lineage con el parser de DuckDB (`json_serialize_sql`) desde Go.
- Entornos = schema o catálogo DuckLake aparte.
- Fases: v1 refresco completo + DAG + expectations + `duct run`; v2 incremental con
  `table_changes()`; v3 streaming continuo y entornos.

## Contratos de datos: ODCS v3.2
- Formato: **ODCS** (Bitol, LF AI & Data, Apache-2.0), en `contracts/<tabla>.odcs.yaml`.
- Librería: **`github.com/alonsoburon/odcs-go`** (propia, Apache-2.0; tipos, validación,
  lint, diff con cambios breaking). Ofrecerla a Bitol.
- Comandos: `duct contract lint | compile | diff | test`.
- Mapeo: `schema` → DDL + CI bloquea cambios breaking; calidad `critical` → `EXPECT … FAIL
  UPDATE`, `warning` → `DROP ROW` o auditoría; reglas `sql` con `schedule` → tarea + alerta;
  SLA `latency`/`frequency` → alerta de frescura por snapshot; `retention` → `duct gc`;
  `roles` → `GRANT`; `team`/`support` → dueño y canal de alertas.
- Extensiones `DUCT` en `customProperties`: DuckLake como servidor `custom`, filtros por fila
  y máscaras.
- Regla de una sola fuente: Bronze y Silver llevan `EXPECT` en el SQL; **Gold y tablas
  compartidas se rigen por su contrato** (genera expectations, grants y alertas).
- ODPS y ontologías de Ossie: diferidos.

## Descartados
- dbt (Jinja/YAML), SQLMesh (58 deps, bugs con DuckLake, actividad baja), datacontract-cli
  (Python, no soporta DuckDB 2.0).
