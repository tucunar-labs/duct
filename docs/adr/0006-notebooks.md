# 0006. Notebooks

Estado: aceptada (2026-10-03)

## Decisión
- **marimo** (Apache-2.0) como `duct notebook`, con el lake conectado; montable vía ASGI en la
  web de Duct con auth propia.
- Duct agrega comentarios e historial (tablas de Duct) y notebooks como tareas
  (`python nb.py --args`).
- Colaboración en tiempo real de marimo (`rtc_v2`) solo como experimental.
- `duct.connect()` funciona también desde Jupyter o un IDE.
- Probado (2026-10-06): marimo 0.25.1 + DuckDB 2.0.0.dev (Python) con DuckLake, `mo.sql`,
  script con args, export, headless, ASGI con auth y psycopg contra Postgres wire.
- **Auth del montaje ASGI: middleware ASGI puro que cubra `http` y `websocket`.**
  `BaseHTTPMiddleware` de Starlette no protege el WebSocket, por donde marimo ejecuta código.
  Test de WebSocket sin token en CI.
- Notebooks como tareas: `python nb.py --x 1` (sin `--`), args con `mo.cli_args()`.

## Descartados
- Jupyter (4–5 proyectos que pegar), Deepnote OSS (incompleto), Positron (Elastic), Zasper (AGPL).
