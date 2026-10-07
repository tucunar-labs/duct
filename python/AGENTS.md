# AGENTS.md (python)

SDK de Python de Duct (`duct-data`, se importa como `duct`). Solo es el SDK de código
de usuario: UDFs (`@duct.function`), tablas Python (`@duct.table`), tareas `python`,
integración con marimo (`duct.notebook.asgi`) y MLflow opcional (`duct.mlflow.score`). El núcleo es Go; ver ADR 0002, 0005, 0006, 0010, 0015.

## Reglas
- Sin credenciales del lake: el SDK solo habla con `duct serve` (HTTP o Postgres wire).
  Nunca S3 ni catálogo directo.
- Stdlib primero: dependencias de runtime vacías. Lo pesado (pyarrow, psycopg, pandas,
  polars, marimo) va en extras opcionales y se importa de forma perezosa con error claro.
- Tipado estricto (pyright strict), código y comentarios en inglés.
- El registro (`duct.registry`) es el contrato con Go: `python -m duct.registry --json`.
  Cambiar su formato JSON exige subir `schema_version` (hoy 2: `duckdb_args`, `duckdb_returns`).
- El worker de UDFs (`duct.udf.worker`) es el otro contrato con Go (`core/udf`): tramas `u32` +
  IPC de Arrow por stdin/stdout. Su stdout es solo del protocolo; no imprimas ahí.
- `duct.notebook.asgi`: el middleware de auth es **ASGI puro** y cubre `http` y `websocket`
  (nunca `BaseHTTPMiddleware`); el test de WebSocket sin token (403) es obligatorio.
- `duct.connect()`: `http(s)://` (ArrowStream con pyarrow, si no JSON) o `postgres(ql)://` (psycopg).

## Verificar (desde `python/`)
```
uv sync
uv run ruff check
uv run ruff format --check
uv run pyright
uv run pytest
```
