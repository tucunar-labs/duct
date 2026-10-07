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
