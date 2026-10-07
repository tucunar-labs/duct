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

## Implementado (2026-10-07): MLflow opcional
- `duct enable mlflow [--listen 127.0.0.1:5000]` agrega `[mlflow]` a `duct.toml` (solo loopback: MLflow no
  tiene autenticación) y, con catálogo Postgres, crea el schema `mlflow` en la base del catálogo.
- `duct mlflow` instala `mlflow==3.17.0` (fijado, `[mlflow] requirement`) en el venv administrado y
  ejecuta `mlflow server` con metadatos en `.duct/mlflow.sqlite` (o en el schema `mlflow` del Postgres del
  catálogo, por `search_path`) y artefactos por proxy en `<lago>/_mlflow` (`file://` o `s3://` con las
  credenciales S3 solo en el entorno del servidor MLflow). La URI del backend viaja por entorno, no por argv.
- `duct query "SELECT * FROM mlflow.runs"`: el CLI hace `ATTACH` de solo lectura (SQLite, o Postgres con
  secreto temporal) en motores sin bloqueo; en `duct serve` el `ATTACH` está cerrado y los roles solo ven
  sus vistas. `duct gc` no toca `lake/_mlflow` (probado).
- **Scoring por lotes**: `duct.mlflow.score("models:/churn/1", name="churn_score", inputs=["DOUBLE",
  "DOUBLE"], columns=["tenure", "spend"])` en `functions/*.py` registra una UDF vectorizada que carga el
  modelo una vez por worker con `mlflow.pyfunc.load_model` y predice por lote; se usa como cualquier
  función en `duct query`, pipelines y jobs. Necesita `MLFLOW_TRACKING_URI`.
- Evidencia: `TestMLflowIntegration` (MLflow real: run, métrica, modelo registrado, `mlflow.runs` por SQL,
  solo lectura, artefactos en el lago, `churn_score` = 10/12/14) y `TestMLflowOnPostgresCatalog`
  (`just test-integration`: tablas solo en el schema `mlflow`). Scoring: 1,4 s para 20 000 filas con el
  modelo pyfunc de prueba (70 µs/fila promedio, incluida la carga única del modelo de ~1 s).
- **Pendiente**: copiar métricas a Bronze desde el orquestador; serving online (`mlflow models serve`).
