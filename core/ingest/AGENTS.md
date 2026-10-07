# core/ingest: concurrencia

Varios procesos `duct ingest` pueden compartir un catálogo Postgres (o SQLite local).

- **Una fuente a la vez.** El estado incremental (marca de agua) vive en el estado de Duct y
  las filas en el lake: dos catálogos, sin transacción común. Si dos procesos leyeran la misma
  marca, ambos añadirían las mismas filas. Por eso `Run` toma un **lease por fuente** en
  `duct.duct_ingest_state` (fila `lock:<fuente>`, `watermark` = dueño, `updated_at` = latido;
  sin migración nueva) antes de leer la marca, y lo suelta al terminar. El `INSERT` contra la
  PK garantiza un único ganador en SQLite y Postgres; los demás esperan (backoff con jitter,
  hasta 15 min). Un latido cada 40 s; un lease sin latido por 2 min se considera de un proceso
  muerto y se toma. Los relojes de las máquinas deben coincidir en segundos.
- **Resultado:** N ingestas simultáneas de la misma fuente se ejecutan en serie; las cuentas
  salen sin duplicados y la marca queda en el máximo. Fuentes distintas no se bloquean entre sí.
- **Unidad de carga atómica y reintentable.** Stage, evolución de esquema y escritura son una
  transacción de lake; ante `Transaction conflict` (p. ej. otra fuente creando el schema
  `bronze` o alterando la misma tabla) se revierte y se repite entera con `core/txretry`,
  sobre los mismos lotes (batch) o repitiendo la consulta remota (SQL). La marca no cambia
  durante los reintentos porque el lease está tomado.
- **Hueco conocido:** si el proceso muere entre el commit del lake y `saveState`, la próxima
  corrida incremental repite esas filas (at-least-once). Cerrarlo exige estado dentro del
  mismo catálogo que los datos (o idempotencia por snapshot), fuera de este alcance.
