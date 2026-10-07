# core/stream: Kafka -> Bronze

`duct stream` (y `[stream]` dentro de `duct serve`) lee los topics de `streams/*.yaml` y los
escribe en `bronze.<tabla>`. ADR 0003. Cliente Kafka: **franz-go** v1.22 (BSD-3, Go puro; el
mismo que usa Bento por debajo), detrás de la interfaz `Consumer` (tests con un falso).

## Definición (`streams/<name>.yaml`, esquema `specs/stream.schema.json`)
`name`, `topic`, `target` (`bronze.<name>`), `format: json`, `batch: {max_rows: 5000,
max_interval: 1s}`, `group` (`duct-stream-<name>`), `start: earliest|latest` (grupo sin
offsets), `inline_rows` (inlining de DuckLake en el schema bronze), `window` (historial que
`duct alerts` guarda en memoria; 0 = las alertas ignoran el stream). Brokers en `[kafka]` de
`duct.toml`; usuario/clave SASL solo por `DUCT_KAFKA_USER` / `DUCT_KAFKA_PASSWORD`.

## Semántica de entrega
- **Offsets después del commit.** Un micro-batch (hasta `max_rows` o `max_interval` desde el
  primer mensaje) se escribe en **una transacción** de DuckLake (con `txretry` ante conflictos);
  solo después `CommitRecords`. Auto-commit apagado. Un crash entre ambos reentrega el batch:
  **at-least-once** desde Kafka.
- **Sin duplicados en Bronze.** Cada fila lleva `_kafka_partition`, `_kafka_offset`,
  `_kafka_ts`, `_kafka_key` e `_ingested_at` (now() del commit). Antes de escribir se descartan
  los mensajes con offset <= máximo ya guardado de esa partición (se lee de la tabla una vez por
  asignación; `OnAssigned` lo invalida), así una reentrega no duplica: efectivamente una vez.
  Queda fuera el productor que publica el mismo evento dos veces (otro offset): deduplicar
  aguas abajo.
- **Un write que falla no se salta.** Se reintenta con backoff (250 ms a 30 s) y el stream se
  detiene: los datos esperan en Kafka. Solo los mensajes que no son un objeto JSON van a
  `bronze.<tabla>_rejects` (partición, offset, texto, error), en la misma transacción.
- Rebalanceo: `BlockRebalanceOnPoll` (no se pierden particiones a mitad de batch); sesión de
  10 s para recuperarse de un crash (`kill -9`).

## Esquema
Tipos inferidos por batch: bool, int64, float64 (enteros y decimales mezclan a float64),
timestamp (todas las cadenas RFC 3339), texto; objetos/arrays anidados se guardan como columna
`JSON`; cualquier otra mezcla cae a texto. Un campo siempre nulo en el batch no crea columna.
Columnas nuevas = `ALTER TABLE ADD COLUMN` (`ingest.EvolveSchema`, igual que la ingesta). Un
cambio de tipo incompatible con la tabla existente falla el write (el stream se detiene hasta
que se arregle). Los campos `_kafka_*` / `_ingested_at` del mensaje se ignoran.

## Gotchas
- `CALL lake.set_option('data_inlining_row_limit', N)` global **segfaultea DuckDB 1.5.6** cuando
  ya hay tablas; por eso `inline_rows` usa `schema => 'bronze'` (probado).
- `Land` (reutilizado por `core/alerts` con el catálogo `memory`) necesita la entrada como vista
  `duct_input` (`Transactor.InTx`).
- Bento -> Kafka: Duct no embebe la salida Kafka de Bento (el módulo `source-bento` solo enlaza
  `pure` e `io`). Se usa Bento por su cuenta (`output: kafka_franz`) o cualquier productor.
- Tests: `cmd/duct/stream_test.go` (consumidor falso: commit tras escritura, reentrega, rejects,
  evolución) y `test/integration/stream_kafka_test.go` (Kafka real en Podman, `kill -9`).
