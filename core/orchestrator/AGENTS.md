# core/orchestrator

Orquestador propio (ADR 0005), dentro de `duct serve` y por CLI (`duct jobs list|run|history`).

- **Jobs:** `jobs/*.yaml` (`specs/jobs.schema.json`): cron + zona horaria, `triggers.on_snapshot`
  / `on_file`, DAG de tareas (`pipeline`, `ingest` integradas; `shell`, `python`, `notebook` son
  módulos `task/*`), reintentos con backoff, timeout, `alerts.on_failure` (canal por registro).
- **Estado** (migración 6): `duct_jobs_runs`, `duct_task_runs`, `duct_jobs_fires`, `duct_jobs_state`.
  No guarda params ni credenciales: cada worker lee `jobs/` de su propio proyecto.
- **Una vez por tick:** la PK de `duct_jobs_fires` (`cron:<job>:<tick>`, `snap:<job>:<id>`,
  `file:<job>:<hash>`) decide quién crea la corrida; `@every` se alinea a la época.
- **Cola:** cada proceso reclama tareas listas con `UPDATE ... WHERE status='pending'` + lectura
  del dueño (un ganador en SQLite y Postgres; `SKIP LOCKED` no es alcanzable a través del
  catálogo adjunto de DuckDB). Latido por proceso; tareas sin latido vuelven a la cola.
  La corrida la cierra (y avisa) un solo proceso, elegido igual.
- **Tareas de usuario:** entorno mínimo + `DUCT_URL`/`DUCT_TOKEN`; nunca credenciales del lake.
- **Pipelines:** pasan por `contracts.RunPipeline` (misma ruta que `duct run`: contratos, roles, vistas).
- **Apagado:** las tareas en curso se cancelan y quedan `cancelled`; la corrida queda `cancelled`.
- **No vigilar** con `on_snapshot` una tabla que el propio job escribe (se dispararía solo).
- Tests con motor real: `cmd/duct/jobs_test.go` y `test/integration/jobs_test.go` (core no importa módulos).
