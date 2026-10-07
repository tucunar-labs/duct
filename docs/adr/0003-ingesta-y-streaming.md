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
