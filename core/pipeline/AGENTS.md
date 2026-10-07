# core/pipeline: concurrencia

Varios procesos `duct run` pueden compartir un catálogo Postgres. Semántica (probada en
`test/integration/concurrent_test.go`):

- **Mismo destino a la vez: gana el último en confirmar, y todos terminan en éxito.** Cada
  refresco es una sola transacción de DuckLake (`CREATE SCHEMA IF NOT EXISTS` + `CREATE OR
  REPLACE TABLE`). Si DuckLake responde `Transaction conflict` (conflicto lógico: no lo
  reintenta él), `core/txretry` repite la transacción entera con backoff exponencial y jitter
  (8 intentos, 50 ms a 2 s). Esto cubre también la carrera de `CREATE SCHEMA`.
- **No se vuelve a evaluar el SELECT al reintentar.** El staging (en memoria, por proceso) se
  calculó con el snapshot que había al empezar; reintentar solo repite la escritura. En un
  refresco completo esto es aceptable: la tabla queda con los datos del proceso que confirmó
  último, que son coherentes (un snapshot de sus fuentes) aunque no sean los más nuevos si otro
  proceso cambió las fuentes entremedio. La siguiente corrida los alcanza. Si en v2 (incremental)
  hay que recalcular, el reintento debe volver al paso de staging.
- Solo se reintentan conflictos de commit. `FAIL UPDATE`, errores SQL y de permisos fallan al
  instante y conservan la versión previa.
- `run_id` (PK junto con el destino en `duct_pipeline_runs`) = microsegundos x 1000 + azar
  0..999, para que procesos lanzados a la vez no choquen (chocaban con solo microsegundos).
