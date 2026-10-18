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
- **Proxy con auth para `http` y `websocket`** (`/nb/{modo}/{nombre}/...` en `modules/web`; desde el mismo día en un origen aparte con cookie propia, ver «Sandbox y origen aparte»; lo que sigue describe sus comprobaciones, hoy con la cookie del notebook en lugar de la de la consola): solo responde con sesión
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
- **Riesgo asumido (resuelto el mismo día, ver «Sandbox y origen aparte» abajo)**: el notebook se servía en el mismo origen de la
  consola y un editor ejecutaba Python como el usuario del servidor (podía leer el lago local y `.duct/`, y un notebook que inyectara
  HTML/JS actuaba con la sesión de quien lo abriera). Eso obligaba a limitar `editors` a admins.
- Evidencia: `cmd/duct/notebooks_console_test.go` — `TestNotebooksConsole` (marimo simulado: visor sin edición, WebSocket sin
  sesión/falsa/otro origen 403, el token del proceso lleva el rol del usuario y no hay otras variables, bloqueo por archivo,
  historial con diff y restauración, logout mata el proceso y revoca el token, comentarios, crear/renombrar/eliminar, auditoría),
  `TestNotebooksIdle` (inactividad), `TestNotebooksMarimo` (marimo real: modo run ejecuta con datos gobernados por rol, modo edición,
  bloqueo, procesos terminan en el logout); pruebas de diff en `core/notebooks`.

## Sandbox y origen aparte (2026-10-18)
Cierra los dos huecos anteriores. Con ambos activos, `editors = ["duct_admin", "analistas"]` es seguro frente al kernel y frente al
HTML/JS de un notebook (los residuales están al final).

**1. Sandbox por proceso** (`core/notebooks/sandbox.go`; `[notebooks] sandbox = "auto" | "bwrap" | "none"`, por defecto `auto`).
- Cada `marimo edit|run` arranca dentro de **bubblewrap** (`bwrap`, detectado en tiempo de ejecución con una prueba real de namespaces):
  `--unshare-all --share-net --die-with-parent --new-session --cap-drop ALL`, `/usr` y los enlaces `/bin /lib /lib64 /sbin` de solo
  lectura, unos pocos archivos de `/etc` (certificados, `resolv.conf`, `hosts`, `passwd`), el **venv de Python solo lectura**
  (más la instalación base a la que apunta `pyvenv.cfg` y los directorios de los `.pth`, p. ej. el SDK editable), `/tmp` y un home
  (`/home/sandbox`) **privados** en tmpfs de 1 GiB (cuentan como memoria; mueren con el proceso, así que nadie lee el tmp de otro),
  su propio `/proc` y `/dev`, y el directorio `notebooks/` montado **de lectura y escritura en edición y solo lectura en ejecución**.
  **Nada más del proyecto**: ni `lake/`, ni `.duct/` (base de estado con hashes, clave de sesión, clave de sharing, catálogo), ni
  `duct.toml`. La carpeta de trabajo es `notebooks/`. Se rechaza un entorno de Python cuyo directorio contenga la raíz del proyecto.
- El entorno sigue siendo la lista blanca de `udf.SafeEnv` más `DUCT_URL`/`DUCT_TOKEN`, con `HOME`, `TMPDIR` y `XDG_*` apuntando al
  tmp privado. La consulta del notebook sigue pasando por `duct serve` con el token del usuario (filtros y máscaras de su rol).
- `--share-net` es necesario para llegar a `DUCT_URL` en loopback. Consecuencia, a la vista: el notebook también alcanza **otros
  servicios en loopback** del host y puede salir a Internet (exfiltrar lo que su rol ve). Aislarlo del todo exigiría un proxy de
  consultas por socket Unix: pendiente.
- Si `bwrap` no está o el kernel no deja crear namespaces: `auto` registra un aviso al arrancar y **solo `duct_admin` puede editar**
  (los demás roles de `editors` se ignoran; el modo ejecución sigue, porque solo ejecuta código que escribió un admin);
  `bwrap` es error de arranque; `none` es la decisión explícita de aceptar el riesgo anterior (aviso al arrancar).
- Detalles de proceso: `--die-with-parent` y el namespace PID hacen que al detener el proceso (logout, inactividad, apagado) muera
  todo el árbol, kernel de marimo incluido.

**2. Origen aparte** (`modules/web/nbauth.go`; `[serve.web] notebooks_listen`, `notebooks_url`).
- Los proxies `/nb/{modo}/{nombre}/...` ya no existen en la consola: viven en un **segundo listener** (`notebooks_listen`, por defecto
  el host de la consola con puerto+1, es decir `127.0.0.1:8485`; `off` los desactiva; si el puerto derivado está ocupado, los
  notebooks se desactivan con un aviso) que no tiene ninguna otra ruta. Tras un proxy inverso hay que darle host propio y declararlo
  con `notebooks_url = "https://nb.ejemplo.com"` (un subdominio hermano de la consola, para que las cookies SameSite lleguen al iframe).
- Entrada por **ticket**: la página `/notebooks/{nombre}` abre el proceso y emite un ticket (cifrado con la clave de sesión con
  propósito `ticket`; **30 s, un solo uso**, atado a usuario, notebook, modo, sesión de la consola y origen de la consola). El iframe carga
  `{origen}/nb-auth?t=...`; ese origen lo canjea una vez, pone su propia cookie `duct_nb` (HttpOnly, `SameSite=Strict`, `Path=/nb/{modo}/{nombre}/`,
  propósito `cookie`; atada a usuario+notebook+modo; **caduca con la inactividad** (la de la sesión) y **con la sesión de la consola**, y deja de valer
  en cuanto esta cierra sesión) y redirige al notebook. La consola nunca acepta `duct_nb` y el origen del notebook nunca lee `duct_session`.
- La consola enmarca solo ese origen (`frame-src` en la página del notebook; el resto conserva `default-src 'self'` y `frame-ancestors 'none'`);
  el notebook solo se puede enmarcar desde el origen de la consola (`frame-ancestors`, tomado del ticket). Cada petición y cada `Upgrade`
  de WebSocket exigen la cookie, `Sec-Fetch-Site` same-origin (o la navegación del iframe, same-site) y `Origin` del mismo host.
- La consola además rechaza lecturas que otro origen haga como script/subrecurso (`Sec-Fetch-Site` ≠ same-origin y modo ≠ navigate), porque en el
  modo por defecto (mismo host, otro puerto) las cookies **no se aíslan por puerto**: el origen del notebook es otro origen, pero no otro
  almacén de cookies. Por eso, en producción, `notebooks_url` con host propio; el arranque lo avisa si no está.
- Probado en Chromium con marimo real: dentro del iframe el JS no lee cookies, no alcanza `window.parent`/`top`, y sus `fetch` a la consola
  los bloquea la CSP del notebook; desde una página same-site ajena, los `fetch` y el `POST /logout` contra la consola y el origen del
  notebook devuelven 403 y ambos se niegan a enmarcarse; con host propio las cookies quedan en jarras distintas.

**Residuales (2026-10-18)**
- **El código de un editor corre con el token de quien lo ejecuta.** El modo ejecución corre `notebooks/x.py` con el rol del visitante: un
  `analistas` que edita puede hacer que un `duct_admin` ejecute código con el rol de admin (dentro del sandbox, pero con red: puede consultar lo
  que el admin ve y sacarlo). Es inherente a notebooks compartidos; mitigaciones: que los admins no abran notebooks de otros sin revisarlos, o
  (pendiente) ejecutar los notebooks de no admin con el rol del autor.
- **Todos los notebooks comparten el origen del notebook**: el JS de un notebook podría hablar con el proceso de otro notebook que la víctima tenga
  abierto (la cookie es por ruta, no un límite de seguridad frente a JS del mismo origen). Un origen por notebook queda pendiente.
- El editor puede cambiar cualquier `notebooks/*.py` (montaje de la carpeta): se registra como versión «externa» del historial, pero el candado por archivo
  no lo impide.
- Tareas `notebook`/`python`/`shell` de jobs y los workers de UDF Python **no** usan bubblewrap (corren como el usuario del servidor, con la lista blanca
  de entorno): son código del repositorio, pero una tarea `notebook` ejecuta un archivo que un editor puede modificar. No tocar el lake desde ahí exige
  revisar esos notebooks o sandboxear `taskrun`.
- `--share-net`, namespaces abstractos de Unix compartidos y el tmpfs de 1 GiB contado como memoria (no hay límite de CPU ni de procesos por notebook).
- El ticket y las cookies se validan en memoria de un solo `duct serve` (un ticket usado vale una vez por instancia).
- La CLI `duct notebook` y el protocolo `notebook` (montaje ASGI) no usan el sandbox: son herramientas de un solo usuario/rol admin.
- Evidencia: `core/notebooks/sandbox_test.go` (el sandbox no ve lake/`.duct/`/`duct.toml`, `notebooks/` solo lectura o escritura, tmp y home privados, `/proc`
  propio, loopback sí, rechazo de un venv que contiene el proyecto, `ResolveSandbox`), `modules/web/nbauth_test.go` (ticket de un solo uso y de 30 s, ligado a la
  sesión, cookie de la consola inválida allí y viceversa, cookie atada a notebook y modo, inactividad, fin con la sesión, sin rutas de consola, `Sec-Fetch-*`),
  `TestNotebooksOrigin`, `TestNotebooksConsole`, `TestNotebooksMarimo` (marimo real en bubblewrap: el notebook informa que solo ve `notebooks/`, consulta con su rol y no
  escribe en la carpeta en modo ejecución) y la comprobación en Chromium descrita arriba.
