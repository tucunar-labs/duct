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

## Implementado (2026-10-18): MLflow a Bronze y serving online
- **`duct mlflow sync [--full]`** copia la base de MLflow (SQLite o schema `mlflow`) a `bronze.mlflow_experiments|runs|params|metrics|tags`
  (columnas explícitas más `*_ts` TIMESTAMP derivados). Idempotente por clave: solo escribe filas nuevas o distintas, así que repetir
  no cambia nada ni crea snapshot. Las marcas de tiempo (`duct_ingest_state`, `mlflow:*`) solo acotan lo que se mira: experimentos y runs
  por `last_update_time` / `max(start, end, deleted)`, parámetros inmutables (solo faltantes de los runs de la ventana), tags reemplazados
  si cambia su (conteo, hash), métricas solo agregadas con `timestamp >= marca - 10 min`. Lease `mlflow-sync` (varios procesos no se pisan).
  Límites: una métrica con timestamp anterior a la ventana o un tag puesto en un run fuera de la ventana entran con `--full`; lo que
  `mlflow gc` borra queda en Bronze (historial); un run con todos los tiempos NULL solo entra en `--full`.
- **`[mlflow] sync = true`** (`sync_interval`, por defecto 5m, mínimo 10s): `duct serve` hace una pasada al iniciar y luego periódica,
  con su propio motor; un fallo se registra y no detiene el servidor.
- **`duct mlflow serve-model models:/<nombre>/<versión> --port N`** ejecuta `mlflow models serve` en `127.0.0.1` (`--env-manager local`,
  venv administrado, `--workers`, `--timeout`), espera `/ping`, revisa su salud cada 15 s y se detiene con SIGINT (gunicorn/uvicorn
  termina lo que tiene en vuelo; kill a los 20 s). Solo recibe `MLFLOW_TRACKING_URI`: resuelve el modelo por el servidor de tracking
  (`duct mlflow` debe estar activo) y no ve credenciales del lake.
- **Proxy en `duct serve`**: con `[mlflow.serving.models.<nombre>] port = N` monta `POST /models/<nombre>/invocations` y
  `GET /models/<nombre>/health` (-> `/ping`); nada más del servidor de MLflow se expone (version, metadata... dan 404). Autenticación
  con token bearer (o Basic), permiso por rol: `[mlflow.serving] roles = [...]`, reemplazable por modelo (`roles` en la tabla del
  modelo); `duct_admin` siempre pasa; sin roles configurados solo `duct_admin`. 401 sin credenciales, 403 con otro rol. Cada llamada
  se audita (`protocol = 'models'`, usuario, rol, estado, duración; nunca el cuerpo ni el token) y cuenta en
  `duct_model_requests_total{model,status}`. El encabezado `Authorization` no llega al backend.
- **Límites y seguridad**: backend solo en loopback (MLflow no autentica: el único camino es el proxy); cuerpo máximo `max_body_mb`
  (16, 413 al pasarlo) y `timeout` por petición (60 s, 504/502 si el modelo no responde); sin límite propio de CPU/RAM: cada worker
  carga el modelo (memoria x `--workers`), así que en producción conviene acotar el proceso con `systemd` (`MemoryMax`, `CPUQuota`) o
  un contenedor. El modelo es código arbitrario (pickle/cloudpickle) con los permisos del usuario que corre `serve-model` y las
  dependencias del venv administrado (no se crea un entorno por modelo): solo modelos de confianza. El rol autoriza llamar al modelo,
  no filtra lo que el modelo hace con sus entradas; las vistas por rol del lake no aplican dentro del modelo. Sin lote/streaming
  de respuesta ni autoescalado; un `serve-model` por modelo.
- **Colector de métricas por servidor**: las series de las tablas de estado (`duct_pipeline_runs_total`, jobs, alertas, snapshot) ya no
  viven en un global que seguía al último `serve` del proceso: cada servidor HTTP tiene su colector (`Server.AddCollector`, caché de 10 s)
  y varios `serve` en un proceso no se pisan. El contador/histograma de proceso (`metrics.Default()`) sigue siendo del proceso.
- Evidencia: `TestMLflowIntegration` (MLflow 3.17.0 real: sync inicial, segunda pasada sin cambios y sin snapshot, runs nuevos,
  run que pasa de RUNNING a FINISHED sin duplicar, tag cambiado, `--full` idempotente, `bronze` igual a `mlflow.*`, serve-model real
  con 200 / 401 / 403 y sync periódico dentro de `serve`), `TestModelProxyAuth` (401/403/200, credencial no llega al backend, 413,
  rutas no expuestas, auditoría), `TestMLflowOnPostgresCatalog` (sync desde el schema `mlflow`), `TestMetricsCollectorPerServer` y
  `TestE2EAdminAPI` ahora en paralelo.
