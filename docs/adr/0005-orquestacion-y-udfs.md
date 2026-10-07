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
- **Límites.** Solo los tipos escalares de arriba (nada de DATE, DECIMAL, LIST…); las funciones
  son escalares (sin agregados ni tablas); `duct serve` registra las funciones al arrancar
  (reiniciar para recargar).
