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

## Implementado (2026-10-07)
- **Ossie 0.2.0.dev0.** JSON Schema vendorizado sin cambios en `core/semantic/schema/` (con
  LICENSE y NOTICE; commit `891f007` de apache/ossie) y validado con `santhosh-tekuri/jsonschema/v6`.
  Otras versiones se rechazan con un mensaje claro. La CLI Go de Ossie no sirve: todo está en
  `internal/` y su `validate` aún no está implementado. Dialectos usados: `OSSIE_SQL_2026` o
  `ANSI_SQL`. Extensión `DUCT`: `{"hidden": true}` en dataset/campo/métrica y
  `{"default_limit", "max_limit"}` en el modelo.
- **Compilador** (`core/semantic`, puro): `Model.Query(Scope{Role, Tables}, Request{dims, measures,
  filters, limit, order})` genera un SELECT sobre `role_<rol>.<tabla>` (`gold.x` → `role_r.x`);
  un dataset sin vista para el rol da `ErrUnavailable`. Cada métrica se agrega sobre su dataset
  "hecho" y solo se une hacia el lado uno de las relaciones (muchos→uno), así que no hay
  duplicación de filas; una dimensión o filtro del lado muchos se rechaza. Varios hechos se
  agregan por separado y se combinan con `FULL OUTER JOIN` sobre las dimensiones. Filtros
  estructurados `{field, op, value}`, literales escapados, nunca SQL del cliente. Límites de la
  primera versión: una métrica vive en un solo dataset (puede referirse a otras métricas del
  mismo); sin filtros sobre métricas (HAVING); sin vistas preagregadas.
- **CLI:** `duct semantic validate|list|build|query` (`query` corre como rol por el
  `governance.Handler`, nunca sobre tablas base).
- **MCP:** `modules/mcp` (`protocol/mcp`, `[serve.mcp]`), SDK oficial
  `modelcontextprotocol/go-sdk` v1.8.0, Streamable HTTP en `127.0.0.1:8765`, bearer = tokens de
  Duct. Herramientas `list_metrics`, `describe`, `query_metrics` y, opcional (`sql = true`), `sql`
  de solo lectura por el mismo guard. Todo se audita (protocolo `mcp`).
