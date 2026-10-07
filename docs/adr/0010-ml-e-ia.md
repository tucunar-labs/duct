# 0010. ML e IA

Estado: aceptada (2026-10-03; Genie 2026-10-04)

## ML
- **MLflow 3.x, opcional** (`duct enable mlflow`): metadatos en la misma base del catálogo
  (`.duct/` en local; schema `mlflow` en el Postgres del catálogo), artefactos en
  `lake/_mlflow/`. Consultable vía `ATTACH`; el orquestador puede copiar métricas a Bronze.
  No escribir MLflow en DuckLake. Es el único servicio nuevo.
- Feature store = tablas DuckLake + `ASOF JOIN`.
- Serving de modelos: batch con UDF DuckDB + `mlflow.pyfunc`; online con `mlflow models serve`.
- AutoML: FLAML.

## IA sobre los datos
- Genie v1 = servidor MCP dentro de `duct serve` sobre las vistas por rol y la capa semántica
  (ADR 0011). El usuario conversa desde su cliente MCP. Chat propio y evaluación de
  precisión: después.
- Funciones IA en SQL: extensión Flock (MIT) o UDF Python del SDK.
- Vector search: columna de embeddings en DuckLake + distancia coseno; LanceDB si escala.
  `vss` (HNSW) experimental.
- Gateway LLM: LiteLLM (librería en el SDK; proxy opcional).

## Descartados
- Feast, BentoML, KServe/Ray, Vanna (archivado), Langfuse (superficie), Phoenix (ELv2), Aim,
  ClearML, PyCaret.
