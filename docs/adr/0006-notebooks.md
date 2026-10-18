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
- **Pendiente** (2026-10-07; el modo edición, los comentarios y el historial se resolvieron el 2026-10-18, abajo): RTC.

## Implementado (2026-10-18)
La consola web trae una sección **Notebooks** (`/notebooks`). Decisión: no se usa `create_asgi_app` (solo modo run);
Duct supervisa **un `marimo edit|run` por usuario, notebook y modo**, y la consola hace de proxy. Servicio en
`core/notebooks` (`contract.NotebooksConsole`, opcional en el `QueryHandler`, como Jobs y Admin).
- **Procesos.** Se crean al abrir el notebook y escuchan en `127.0.0.1:<puerto aleatorio>` con `--base-url
  /nb/<modo>/<nombre>` y `--token-password-file -` (la clave de marimo es aleatoria por proceso, entra por stdin: no
  aparece en `ps`, y solo el proxy la conoce). Entorno con lista blanca más `DUCT_URL` y `DUCT_TOKEN`: un token **del
  usuario**, de corta vida (`max_age`, 12 h), que se revoca al terminar el proceso. Las consultas del notebook corren
  con el rol del usuario (filtros y máscaras incluidos); nunca hay credenciales del lago. Se matan por inactividad
  (`idle_timeout`, 30 min sin peticiones por el proxy), al llegar a `max_age`, al cerrar sesión (`/logout` detiene todos los
  procesos del usuario), cuando el proceso muere solo y al apagar `duct serve`. Primer arranque: crea el venv gestionado
  si hace falta (`preparePython`). Máximo `max_kernels` procesos vivos.
- **Proxy con auth para `http` y `websocket`** (`/nb/{modo}/{nombre}/...` en `modules/web`): solo responde con sesión
  válida cuyo usuario tenga proceso para ese notebook y modo; sin sesión, 401 (http) y 403 antes del `Upgrade`
  (WebSocket). Además `Sec-Fetch-Site`/`Origin` del mismo origen también en el upgrade (otro `Origin`: 403). Se quitan
  `Cookie`, `Authorization` y `Referer` del navegador y se inyecta la clave de marimo; se descartan `Set-Cookie`, CSP y CORS
  de marimo. Las páginas de `/nb/` llevan su propia CSP (marimo necesita scripts inline, `eval` y workers `blob:`) y solo se
  pueden enmarcar desde Duct (`frame-ancestors 'self'`); el resto de la consola conserva la CSP estricta.
  Probado con marimo 0.25.1: `/ws` pasa por el proxy y, en modo run, ejecuta el notebook al conectar.
- **Roles.** `[notebooks] editors = ["duct_admin", "analistas"]` (por defecto solo `duct_admin`): esos roles abren modo
  edición, ven el historial (muestra código) y restauran; el resto abre modo ejecución (código oculto, su propio proceso y token,
  con su gobierno) y comenta. Un visor que pide `?mode=edit` recibe 403 (auditado); `/nb/edit/...` sin proceso propio de
  editor también. Eliminar es solo `duct_admin`; crear y renombrar, editores.
- **Bloqueo por archivo.** Un solo editor por notebook: el primero en abrir en modo edición tiene el candado (su proceso);
  el segundo recibe modo ejecución y un aviso con quién edita. Renombrar, eliminar y restaurar fallan con el candado ajeno.
  Se libera cuando su proceso termina (cierre de sesión, inactividad). El candado vive en memoria de este `duct serve`:
  con varias instancias del servidor no se comparte (los procesos tampoco).
- **Historial.** Migración 12: `duct_notebook_versions` (hash SHA-256, autor, acción, instante, tamaño, líneas +/−,
  contenido completo) y `duct_notebook_comments`. marimo guarda el `.py`: mientras hay un proceso de edición, Duct revisa el
  archivo cada 2 s (mtime/tamaño, luego hash) y registra una versión a nombre del dueño del candado; también al detener el proceso.
  Los cambios hechos fuera de Duct (git pull) se registran como «externo» al abrir el historial; abrir en edición sin versiones
  previas guarda una versión inicial. Diff de líneas (LCS con contexto de 3 líneas) entre versión y anterior o contra el archivo
  actual. **Restaurar** guarda antes el estado actual, escribe el archivo (lo crea si se eliminó) y registra la restauración como
  versión nueva: nada se pierde. Eliminar conserva historial y comentarios. Archivos de más de 4 MB no se versionan.
- **Comentarios** por notebook (opcionalmente con una referencia libre, p. ej. «celda 3»: marimo no guarda ids de celda en el
  archivo, no hay ancla estable), panel lateral junto al notebook embebido (Datastar, sin recargar el iframe), autor y hora, resolver
  y reabrir (el autor o un rol editor), todo auditado (`NOTEBOOK comment|resolve comment ...`; el texto del comentario no entra al audit).
- **Auditoría**: `NOTEBOOK open mode=... name=...`, `create`, `rename`, `delete`, `restore`, `diff`, `comment`, `resolve|reopen comment`;
  los rechazos salen como `denied`.
- **Plantilla** de notebook nuevo con `duct.connect()`.
- **Riesgo asumido, a la vista**: el notebook se sirve en el **mismo origen** de la consola (como pidió el diseño) y un editor ejecuta
  Python en el host con el usuario del servidor: puede leer los archivos del proyecto, incluido el lago local, y un notebook que
  inyecte HTML/JS podría actuar con la sesión de quien lo abra. Por eso editar es de roles de confianza (`editors`) y el lago
  remoto con credenciales de la consola es lo recomendable en producción. Probado: `sandbox` sin `allow-same-origin` rompe marimo
  (localStorage), así que el aislamiento real exige otro origen (subdominio dedicado) o un contenedor por usuario: pendiente.
- Evidencia: `cmd/duct/notebooks_console_test.go` — `TestNotebooksConsole` (marimo simulado: visor sin edición, WebSocket sin
  sesión/falsa/otro origen 403, el token del proceso lleva el rol del usuario y no hay otras variables, bloqueo por archivo,
  historial con diff y restauración, logout mata el proceso y revoca el token, comentarios, crear/renombrar/eliminar, auditoría),
  `TestNotebooksIdle` (inactividad), `TestNotebooksMarimo` (marimo real: modo run ejecuta con datos gobernados por rol, modo edición,
  bloqueo, procesos terminan en el logout); pruebas de diff en `core/notebooks`.
