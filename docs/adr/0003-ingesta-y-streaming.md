# 0003. Ingesta y streaming

Estado: aceptada (2026-10-02; streaming 2026-10-03)

## Decisión
- SQL y archivos: **DuckDB + extensión ODBC** (Oracle, HANA, Progress, DB2…; drivers del
  fabricante). Archivos y S3/HTTP nativos de DuckDB.
- APIs: **Bento** (MIT) **embebido como librería Go** (`service.NewStreamBuilder`), no como
  subproceso: REST/SOAP/GraphQL vía `http_client`, `websocket`, `grpc_client`.
- Streaming: **Bento → Kafka → Duct writer → DuckLake Bronze**. Kafka es el log durable;
  todo dato entra por Bronze (DuckLake es la verdad).

## Capa de Duct sobre Bento (MVP ~2–3 semanas)
- Mensajes → Arrow por lotes → DuckLake; evolución de esquema con `ALTER TABLE`.
- Estado incremental en tablas de Duct, guardado solo si el pipeline termina bien.
- Paginación: interpolación en `http_client`, o `generate` + procesador `http` si hay lógica.
- Duckle (`slothflowlabs/duckle`) como diseño de referencia, no como dependencia.

## Fuera de alcance
- CDC por ahora.

## Descartados
- dlt (columnas de metadatos), Sling (APIs/CDC son Pro), ADBC (driver Oracle de Columnar es
  propietario), Kestra (orquestador, sin SOAP/gRPC/WSS), SeaTunnel/NiFi/Camel (JVM).
- NATS como log (Proton/Spark sin conector nativo en OSS); RabbitMQ (cola, no log).

## Implementado (2026-10-07): Kafka → Bronze
- `duct stream` (y `[stream]` en `duct.toml` para correrlo dentro de `duct serve`): un
  consumer group por `streams/*.yaml` (`topic`, `target`, `batch: {max_rows, max_interval}`,
  `window`, `inline_rows`; `specs/stream.schema.json`). Cliente **franz-go** v1.22 (BSD-3, Go
  puro). Brokers en `[kafka]`; SASL/TLS por env. Código en `core/stream` (ver su AGENTS.md).
- Entrega: micro-batch → **una transacción** de DuckLake (con `txretry` y evolución de esquema
  como la ingesta) → **recién entonces** commit de offsets. At-least-once desde Kafka; cada
  fila lleva `_kafka_partition/_offset/_ts/_key` e `_ingested_at`, y se descartan los mensajes
  con offset ya guardado, así una reentrega no duplica. Mensajes que no son un objeto JSON
  van a `bronze.<tabla>_rejects`; un write que falla se reintenta sin saltarse nada.
- Medido (Kafka 4.3.1 en Podman, SQLite como catálogo, `max_interval: 200ms`): del productor a
  Bronze mediana 205 ms, máx. 650 ms; `kill -9` a mitad de 4 000 mensajes: 4 010 filas,
  0 duplicados, offsets contiguos, lag del grupo 0 tras reiniciar.
- Bento → Kafka: Duct no embebe la salida Kafka de Bento (`source-bento` solo enlaza `pure` e
  `io`, sin Kafka); para APIs en streaming se corre Bento con `output: kafka_franz` (o
  cualquier productor) hacia el topic.
- Pendiente: Avro/Protobuf con registry, `max_interval` adaptativo, particiones con DLQ por
  tipo incompatible, métricas.
