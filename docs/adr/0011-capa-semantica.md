# 0011. Capa semántica

Estado: aceptada (2026-10-04)

## Decisión
- **Apache Ossie** (ex-OSI, incubadora Apache, Apache-2.0) en `semantic/*.ossie.yaml`,
  validado con su JSON Schema en CI; extensiones en `custom_extensions` con
  `vendor_name: DUCT`. Importar Metric Views de Databricks/dbt/Snowflake/Cube con sus
  conversores.
- `duct semantic build` (Go) compila a vistas DuckDB **sobre las vistas por rol** (hereda
  filtros y máscaras); cuidar joins que duplican filas. Opcional: vistas preagregadas
  materializables en los pipelines.
- Consumo: BI y dashboards vía vistas SQL; el MCP expone `list_metrics`, `describe` (con
  `ai_context`) y `query_metrics(dims, measures, filters)`: el LLM no escribe joins.
- Complementa a ODCS (ADR 0004): ODCS describe tablas; Ossie, métricas.

## Riesgos
- Spec 0.x; sintaxis de consulta (`MEASURE()`) en discusión. Futuro: yardstick.
- Revisar si la CLI Go de Ossie expone paquetes reutilizables antes de escribir otra librería.

## Descartados
- MetricFlow (exige dbt), Cube y Wren (servicios), Malloy (Node), Boring Semantic Layer (Python).
