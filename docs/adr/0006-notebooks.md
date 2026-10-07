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

## Implementado (2026-10-07)
- **Entorno de Python** (decisión): si el proyecto trae el suyo (`pyproject.toml` con `uv`, `[python] command`
  en `duct.toml` o `DUCT_PYTHON`), se usa tal cual y un módulo faltante es un error que dice qué agregar;
  si no, Duct administra un venv en `.duct/venv`, creado con `uv` al primer uso, donde instala el SDK
  (`[python] sdk` o `DUCT_SDK`, por defecto `duct-data[marimo,arrow,pg]`) y lo que falte.
- **SDK** (`python/duct-data`): `duct.connect()` elige transporte por el esquema de `DUCT_URL`:
  `http(s)://` usa `FORMAT ArrowStream` (con `pyarrow`; si no, JSON) y `postgres(ql)://` usa `psycopg`
  (la contraseña puede venir en `DUCT_TOKEN`; el protocolo Postgres no acepta tokens). `Result.to_arrow()`,
  `.to_pandas()`, `.to_polars()` y `.rows`.
- **`duct notebook [archivo] --user U`**: levanta un `duct serve` privado (solo HTTP, 127.0.0.1, sin jobs ni
  streams), emite un token de 12 h para el usuario (se revoca al salir) y ejecuta `marimo edit` en 127.0.0.1
  con `DUCT_URL`/`DUCT_TOKEN` y un entorno con lista blanca (nunca credenciales del lago). También
  `--url/--token` para un servidor ya corriendo, y `--script ... -- --x 1`, que lo ejecuta como script
  (como lo hace la tarea `notebook`). Evidencia: `TestNotebookScript`.
- **Montaje con auth** (`[serve.notebook]`, protocolo `notebook`, no arranca por defecto): `modules/notebook`
  supervisa `python -m duct.notebook.asgi`, que sirve `notebooks/*.py` con `marimo.create_asgi_app()`
  (modo run, solo lectura: el visitante ejecuta el código del proyecto, no el suyo) detrás de un
  middleware ASGI puro que cubre `http` y `websocket`. Credenciales: `Bearer`, `Basic`, cookie `duct_nb`
  o `?token=`; las valida Go (`AuthProvider`) por un endpoint loopback con secreto por proceso. Sin
  credenciales: 401 en HTTP y cierre antes del `accept` (403) en WebSocket; también 403 con otro
  `Origin`, y 429 tras 5 contraseñas malas por usuario. Un solo rol entra (`role`, por defecto `duct_admin`)
  y es el mismo con que consultan los notebooks (`DUCT_NOTEBOOK_TOKEN`), así que no hay escalada.
  Escucha solo en loopback; TLS va en un proxy. Evidencia: `TestServeNotebookAuth` (WebSocket sin token,
  con token malo, de otro rol y de otro origen: 403; con token: 101) y `python/duct-data/tests/test_asgi.py`.
- **Pendiente**: modo edición en el montaje (`create_asgi_app` solo monta apps en modo run; el modo edición
  exigiría API internas de marimo; hoy se edita en local con `duct notebook`); comentarios e historial en tablas de Duct; RTC.
