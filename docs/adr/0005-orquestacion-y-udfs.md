# 0005. Orquestación y UDFs

Estado: aceptada (2026-10-03)

## Orquestación propia (~3–4 semanas), dentro de `duct serve`
- Cron (`robfig/cron`), DAG propio, tablas `runs`/`task_runs`, reintentos con backoff,
  timeouts, triggers por snapshot de DuckLake o archivo nuevo.
- Multi-máquina sobre Postgres: lock para cron único, cola con `SKIP LOCKED`, heartbeats.
- Durabilidad a nivel de tarea (cada refresco es un commit atómico de DuckLake).
- Tipos de tarea: tabla/pipeline, `python`, `shell`, notebook (idempotencia a cargo del
  usuario). Reanudar a mitad de una tarea arbitraria: futuro (DBOS candidato).

## UDFs
- Funciones Python de DuckDB (escalares y vectorizadas con Arrow) en `functions/*.py` con
  `@duct.function`; también ejecutables stdin/stdout al estilo de las executable UDF de
  ClickHouse.

## Descartados
- Dagster (daemon), Prefect (servidor), Airflow, Kestra. DBOS/Huey/Absurd como plan B.

## Implementado (2026-10-07): UDFs
- **Mecanismo.** `duckdb-go` sí permite UDFs escalares en Go (`RegisterScalarUDF`, ejecutor por
  chunk). El contrato `Engine` gana la interfaz opcional `ScalarRegistrar` (`core/contract/v1/udf.go`)
  y `engine-duckdb` la implementa: cada chunk de DuckDB (2048 filas) pasa a Arrow, se llama a la
  función una vez y el resultado vuelve al vector. Las filas con NULL no llegan a la función
  (NULL entra, NULL sale). Tipos: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR.
- **Python** (`functions/*.py`, `@duct.function`): `core/udf` descubre las funciones con
  `python -m duct.registry --json` (esquema 2: `duckdb_args`/`duckdb_returns` salen de las anotaciones
  `int/float/str/bool` o de `types=[...]`, `returns=...`) y las atiende un pool de procesos
  `python -m duct.udf.worker` (2 por defecto, `[python] workers`, arrancan al primer uso). Protocolo por
  stdin/stdout: trama `u32` + nombre + stream IPC de Arrow, respuesta con estado y stream IPC; la
  salida estándar del usuario se desvía a stderr. `vectorized=True` recibe y devuelve arrays Arrow;
  si no, una llamada por fila. Un error de la función es un error de la consulta con su motivo; un
  timeout (60 s) mata al worker y el pool lo reemplaza.
- **Ejecutables** (`functions/<nombre>.toml`: `command`, `args`, `returns`, `format = "tsv"|"json"`,
  `timeout`): un proceso por lote, una fila por línea en stdin y una respuesta por línea en stdout
  (se exige el mismo número de líneas). Sin Python.
- **Dónde corren.** `duct query|sql|run|jobs` (CLI local, de confianza) y `duct serve` solo para el rol
  `duct_admin`: el guard (`core/governance/udf.go`) rechaza los nombres registrados para cualquier otro
  rol, en cualquier posición de la consulta (SELECT, WHERE, subconsulta, CTE, ventana, lambda). Por
  el protocolo Postgres no existen (sesión DuckDB propia de duckgres). Los procesos reciben un entorno
  con lista blanca: sin `DUCT_*`, `AWS_*` ni credenciales del catálogo. Un `functions/` que no carga es
  un aviso en el CLI y un error en `duct serve`.
- **Costo medido** (Ryzen 5 8645HS, `go test ./core/udf -bench PythonBatch`, ida y vuelta Arrow por lote):
  ~0,13 ms fijos por lote; vectorizada 0,17 ms para 2048 filas y 1,6 ms para 65 536; escalar Python
  0,7 ms por 2048 filas (~0,35 µs/fila). De punta a punta con DuckDB (`duct functions bench`, 200 000
  filas): vectorizada 191 ns/fila (0,4 ms por lote de 2048), escalar Python 1,75 µs/fila (3,6 ms),
  ejecutable 0,83 µs/fila (1,7 ms: el costo es lanzar el proceso por lote).
- **Límites.** (Superado el 2026-10-12, ver más abajo: tipos, tablas y recarga. Siguen sin existir los
  agregados.)

## Implementado (2026-10-07): recarga, retención y ticks perdidos
- **Recarga en caliente de `jobs/*.yaml`** en `duct serve`: se sondea la carpeta (hash del
  contenido, 2 s), se parsea y valida el conjunto completo y `Orchestrator.Reload` cambia el
  catálogo de jobs de forma atómica; un archivo inválido se registra y todo sigue como estaba.
  Las corridas en curso terminan (las de un job borrado con su última definición conocida; las
  tareas pendientes de un job editado toman los parámetros nuevos); un job nuevo o con otro
  `schedule`/`timezone` empieza su cron desde ahora, los triggers de un job nuevo toman su línea
  base en el siguiente sondeo (no disparan por lo que ya existe). Un servidor sin jobs
  empieza a servirlos cuando aparece el primero. Evidencia: `TestJobsHotReload`.
- **`catchup: none|last|all`** por job (por defecto `last`, esquema en `specs/jobs.schema.json`). El
  último tick tratado se guarda en `duct_jobs_state` (`cron:<job>`) y al arrancar se retoma desde
  ahí: `none` lo olvida; `last` lanza una corrida por el tick más reciente perdido (lo que ya hacía
  la coalescencia en memoria, ahora también tras un reinicio); `all` lanza uno por tick perdido, del
  más viejo al más nuevo y **de a uno** (espera a que termine la corrida activa), con tope de 100
  ticks (se descartan los más viejos) y ninguno de más de 6 días (las claves de `duct_jobs_fires` se
  guardan 7). La primera vez que corre un job no hay estado y no se repite nada. Con varias máquinas
  cada tick sigue creando una sola corrida (la clave `cron:<job>:<tick>` de `duct_jobs_fires`); las
  corridas de recuperación llevan `detail = catch-up`. Evidencia: `TestJobsCatchup` (dos servidores
  sobre el mismo estado: `last` 1 corrida, `all` 4–6 en orden, `none` 0).
- **Retención del historial:** `[jobs] history_retention = "30d"` (sin valor o `0` conserva todo)
  borra corridas terminadas y sus intentos al arrancar, cada hora en `duct serve` y en `duct gc`
  (`project.PruneJobsHistory`); nunca toca corridas en curso y las claves de disparo se guardan
  al menos 7 días. Evidencia: `TestJobsHistoryRetention`.

## Implementado (2026-10-12): tipos, funciones de tabla y recarga de `functions/`
- **Tipos.** Además de los escalares: `DATE`, `TIMESTAMP` (microsegundos, sin zona), `DECIMAL(p,s)`
  (p ≤ 38), listas `T[]` (anidables) y `STRUCT(a T, ...)`, como argumentos y retornos. Una sola
  gramática (`contract.ParseSQLType` en Go, `duct/udf/types.py` en Python; la forma canónica es
  `BIGINT[]`, `DECIMAL(10,2)`, `STRUCT(n BIGINT, s VARCHAR[])`). Mapeo Arrow: date32, timestamp[us],
  decimal128, list, struct. Un NULL de nivel superior sigue sin llegar a la función (NULL entra, NULL
  sale); los NULL dentro de listas y structs sí llegan.
  - Python escalar recibe/devuelve `datetime.date`, `datetime.datetime`, `decimal.Decimal`, `list`, `dict`;
    la vectorizada recibe arrays Arrow. Las anotaciones resuelven `date`, `datetime`, `list[int]`…; DECIMAL y
    STRUCT no tienen anotación posible: `types=[...]`/`returns=...`.
  - Ejecutables (`functions/*.toml`): DATE (`2026-01-31`), TIMESTAMP (`2026-01-31 12:30:00.123456`) y
    DECIMAL (texto exacto, `-123.45`) funcionan en `tsv` y `json` (en json el decimal es un número JSON, y al
    volver acepta número o texto). Listas y structs solo con `format = "json"` (en `tsv` el `.toml` se rechaza al
    cargarlo); en json un STRUCT va como objeto, una lista como arreglo, un NULL anidado como `null`.
- **Funciones de tabla** (`@duct.table_function(columns={"n": "BIGINT"}, types=[...])`): `duckdb-go` sí
  expone `RegisterTableUDF`; el contrato gana `TableRegistrar`. Los argumentos son constantes de la
  consulta; la función corre una vez al enlazar y devuelve `pyarrow.Table`/`RecordBatch`, un generador
  de ellos, un dict de columnas o una lista de filas-dict (las columnas se emparejan por nombre y se
  castean al esquema declarado); DuckDB las recibe en trozos de 2048 filas. Funcionan en `duct query|sql|run|jobs`
  (también dentro de un pipeline); en `duct serve` el guard rechaza todas las funciones de tabla (también al
  admin), como ya hacía. Sin argumentos con nombre.
- **Agregados: no se implementan.** `duckdb-go` v2.10506 (y la preview 2.20000) no trae
  `RegisterAggregateUDF` ni binding de `duckdb_create_aggregate_function` (solo está en `duckdb.h`). Alternativa
  disponible hoy: `mi_funcion(list(x))` con un argumento `DOUBLE[]` y una función escalar vectorizada o no.
  Reabrir si duckdb-go lo expone.
- **Recarga en caliente** en `duct serve` (`udf.Manager.Reload`, `cmd/duct/functions_serve.go`): se sondea
  `functions/*.py|*.toml` (hash del contenido, 2 s, mismo mecanismo que jobs y semántica); se descubre
  el conjunto completo (`python -m duct.registry`) y **solo si pasa** se aplica; si no, se registra el error y todo
  sigue como estaba. Como DuckDB no puede desregistrar una función (volver a registrar la misma firma la reemplaza; otra firma
  crea una sobrecarga), cada función registrada es un envoltorio que reenvía a su implementación actual, que la recarga cambia de forma atómica:
  - cuerpo nuevo: se aplica a las llamadas siguientes; el pool de workers Python se reemplaza: las llamadas
    en curso terminan en el worker viejo (que se detiene al devolverlo) y las nuevas arrancan workers con el código nuevo;
  - nombre nuevo: se registra en el motor (y el guard `AdminOnlyFunctions` lo conoce **antes** de que sea llamable:
    `Handler.SetAdminOnlyFunctions`); un servidor que arrancó sin `functions/` toma la primera;
  - nombre que desaparece: DuckDB conserva el nombre; la función responde `was removed from functions/ (restart
    duct serve to drop it)` y sigue siendo solo-admin hasta reiniciar; si vuelve a aparecer, funciona de nuevo;
  - cambio de firma (tipos de argumentos/retorno, columnas de una función de tabla) de un nombre existente: se
    rechaza la recarga entera con "restart duct serve" (DuckDB, al registrar otra firma con el mismo nombre, crea una sobrecarga y deja viva la vieja).
  Evidencia: `TestServeFunctionsHotReload`, `TestServeFunctionsHotReloadFirstFile`, `TestManagerReload`,
  `TestManagerReloadDrainsInFlight`.
- **Evidencia de tipos y tablas:** `TestE2EFunctionsTypesAndTables`, `TestUDFComplexTypes`, `TestRegisterTable`
  (motor), `TestExecComplexTypes`, `TestPythonTypesAndTableFunctions`, `TestParseSQLType` (core/udf),
  `test_complex_types`, `test_table_functions`, `test_type_grammar` (pytest). El esquema del registro Python sube a 3 (`kind`, `columns`).
- **Límites.** Sin agregados; sin argumentos con nombre en funciones de tabla; sin TIMESTAMP WITH TIME ZONE,
  MAP, UNION, ENUM, DECIMAL > 38; la recarga no cubre helpers importados desde `functions/` (solo se
  observan `*.py` y `*.toml` de esa carpeta, y se cargan todos como módulos de usuario) ni cambios de firma.
- **Sandbox (2026-10-18, ver ADR 0006 «Menor privilegio de los autores y sandbox de tareas»).** Las tareas `python` y `notebook` de jobs y los workers Python de UDF (también
  la carga de `functions/*.py`) corren en bubblewrap por defecto: proyecto de solo lectura sin `.duct/`, `duct.toml`, `.git`, `.env*` ni el lake local, `/tmp` privado, un directorio
  escribible opcional (`params.writable`) y red compartida para `DUCT_URL`. `[sandbox] mode|jobs|udf|shell` en `duct.toml`; `shell` es opt-in; `sandbox: false|true` por tarea; sin bwrap,
  `auto` avisa y corre sin aislar, `bwrap` falla; `uv run` no cabe (intérprete directo en `[python] command`). Una tarea `notebook` solo arranca si ningún autor de otro rol modificó el
  notebook después de la última revisión de la identidad del job (`duct notebook approve`); si no, falla sin ejecutar nada. Evidencia: `TestJobSandbox`, `TestNotebookJobGuard`,
  `TestPythonSandboxed*` (core/udf). UDFs ejecutables (`functions/*.toml`) siguen sin bubblewrap.
