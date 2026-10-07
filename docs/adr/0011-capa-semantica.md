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
  mismo); sin filtros sobre métricas (HAVING); sin vistas preagregadas (todo esto sigue abajo).
- **CLI:** `duct semantic validate|list|build|query` (`query` corre como rol por el
  `governance.Handler`, nunca sobre tablas base).
- **MCP:** `modules/mcp` (`protocol/mcp`, `[serve.mcp]`), SDK oficial
  `modelcontextprotocol/go-sdk` v1.8.0, Streamable HTTP en `127.0.0.1:8765`, bearer = tokens de
  Duct. Herramientas `list_metrics`, `describe`, `query_metrics` y, opcional (`sql = true`), `sql`
  de solo lectura por el mismo guard. Todo se audita (protocolo `mcp`).

## Implementado (2026-10-07): métricas entre hechos, HAVING, preagregados y recarga
- **Métricas derivadas entre datasets.** Una métrica cuya expresión solo combina otras métricas
  de hechos distintos (`revenue / units`, cada una en su fact) es *derivada*: cada métrica base se
  agrega en su propio grano a las dimensiones pedidas (los CTE `q0`, `q1`…, sin fan-out) y la
  derivada se calcula **después** del `FULL OUTER JOIN` por dimensiones, sobre las columnas ya
  agregadas. Mezclar campos (`SUM(a.x) + SUM(b.y)`) o agregados con métricas de otro hecho sigue
  siendo error de validación. Hace falta una dimensión conformada: una dimensión que un hecho no
  alcanza se rechaza igual que antes. Un rol necesita las vistas de todos los hechos que usa;
  `describe`/`list_metrics` solo ofrecen las dimensiones que alcanzan todos. Tests de oro:
  `core/semantic/testdata/derived_ratio.sql`; con datos reales `TestSemanticDerivedHavingAndPreaggregates`
  (CL 450/15 = 30, AR 200/1 = 200, sin inflar por las líneas de pedido).
- **Filtros sobre métricas (HAVING).** `{metric, op, value}` junto a `{field, op, value}` (el
  contrato `SemanticFilter` gana `Metric`, aditivo; MCP `query_metrics` y `duct semantic query
  --having 'revenue>300'`). La métrica puede no estar entre las pedidas: se calcula como columna
  auxiliar (`__h0`, nunca llega al resultado) y se filtra sobre el resultado combinado. Se validan
  nombre, visibilidad, permiso del rol sobre sus hechos y operador (`like`/`not_like` no aplican);
  field y metric a la vez es error.
- **Preagregados.** Extensión `DUCT` del modelo:
  `{"preaggregate": {"name": "ventas", "dims": ["customers.region"], "measures": ["revenue"], "refresh": "on_snapshot"}}`
  (objeto o lista; `refresh`: `on_snapshot` por defecto o `manual`). Se validan al cargar el modelo
  (campos, métricas, joins sin fan-out). **Decisión de gobierno: una tabla por rol, construida desde
  las vistas del rol** (`semantic_cache.<rol>__<nombre>`), así los filtros de fila y las máscaras ya
  están aplicados en los datos; nunca se comparten agregados entre roles ni se usan los de otro.
  Ningún camino del guard abre `semantic_cache`: solo el SQL que compila la capa semántica la lee
  (`Handler.semanticQuery`, marca interna de contexto) y solo cuando la tabla es **fresca**: misma
  definición (hash del SQL de construcción, que incluye las expresiones del modelo), mismas
  definiciones de las vistas del rol (hash de `duckdb_views()`; una política nueva reescribe las
  vistas) y ningún snapshot del lake posterior a su origen salvo los de las propias construcciones
  (`CALL lake.set_commit_message('duct', 'semantic_cache')`). En cualquier duda se lee de las
  vistas. El planificador elige el preagregado más chico que cubra la consulta: dimensiones y
  filtros de campo ⊆ dims y medidas ⊆ measures; con las mismas dimensiones lee la tabla tal cual,
  con menos las re-agrega solo si todas las medidas son un `SUM/COUNT/MIN/MAX` sin `DISTINCT`
  (el resto —`AVG`, `COUNT(DISTINCT)`, razones, derivadas— exige coincidencia exacta de dimensiones).
  Estado en `duct.duct_semantic_preagg` (migración 10). Se (re)construyen tras `duct run` y tras
  cada job de pipeline (`contracts.RunPipeline`, después de regenerar las vistas), por `duct semantic
  build [--role R] [--force]` y en `duct serve` cada 15 s o al cambiar el modelo; `duct semantic
  status` dice cuáles servirían ahora; `duct semantic build --sql --role R` conserva el modo de
  imprimir el SQL de cada métrica. Quitar un preagregado del modelo (o un rol) borra sus tablas.
  Evidencia: `TestSemanticDerivedHavingAndPreaggregates` (un rol sin filtro construye su tabla; al
  endurecer su política la tabla queda obsoleta **al instante**, la consulta vuelve a las vistas
  filtradas y no aparece la fila `void`; el rol `analistas`, con filtro, lee solo `paid` desde su
  propia tabla; un `INSERT` también la vuelve obsoleta), `TestServeSemanticHotReload` (serve la
  construye sola y la usa) y `core/semantic` (`TestPreaggregateRouting`, goldens `preagg_*.sql`).
  Límite: una tabla por rol y preagregado (crece con roles × preagregados) y reconstrucción
  completa, no incremental.
- **Recarga en caliente.** `core/semantic.Live` sondea `semantic/*.ossie.yaml` (hash del contenido
  cada 2 s, sin fsnotify), valida y resuelve el modelo completo antes de cambiarlo y, si es
  inválido, deja el anterior y lo registra. `duct serve` comparte un `Live` con el handler
  (dashboards, preagregados) y con MCP (`cfg["semantic_live"]`; MCP ahora delega `query_metrics`
  en `SemanticQuerier`, así también usa preagregados y auditoría única). Si se borran todos los
  archivos el modelo queda en nada; MCP solo registra las herramientas semánticas si había modelo
  al arrancar. Evidencia: `TestLiveReload`, `TestServeSemanticHotReload`.
