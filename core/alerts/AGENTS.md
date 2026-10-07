# core/alerts: alertas en tiempo real

`duct alerts` (y `[alerts]` dentro de `duct serve`). ADR 0009.

## Cómo funciona
1. **Entrada.** Consumer group propio (`[alerts] group`, por defecto `duct-alerts`) sobre los
   topics de los streams con `window:`. No confirma offsets: cada arranque o rebalanceo repite
   los últimos `window` minutos desde Kafka (`AfterMilli`), que es justo el estado que hace falta.
   Escalar = más procesos en el grupo; Kafka reparte por partición, así que hay que **particionar
   por la clave de la alerta** (p. ej. sensor).
2. **Ventana.** Un DuckDB en memoria aparte (`engine-duckdb` con `"memory": true`, sin lake) con
   una tabla por stream, del nombre del stream, con las columnas del JSON + `_kafka_ts`,
   `_kafka_offset`, `_kafka_partition`, `_kafka_key`, `_ingested_at`. Se barre cada 1 s por
   `_kafka_ts`. Una regla de "sensor mudo" necesita un umbral menor que `window`.
3. **Reglas** `alerts/<nombre>.sql`: comentarios de cabecera `-- EVERY 200ms`, `-- KEY col`,
   `-- SEVERITY info|warning|error|critical`, `-- CHANNELS ntfy:incendios, webhook:ops`,
   `-- MESSAGE {col} marca {temp}` (plantilla; también `{rule}` y `{key}`). Un SELECT cuyas filas
   son las alertas disparadas ahora, una por clave. Sin `KEY` y sin columna `key`: una alerta
   global. Cada regla tiene su ticker y evalúa en su propia transacción de solo lectura.
4. **Estado** por (regla, clave): no activa -> `firing` -> `resolved` (`Tracker`). Mientras la
   clave siga en el resultado no hay más avisos (dedup); al salir se avisa `resolved`. Una
   evaluación con error no cambia nada (no resuelve por accidente). Antes de la primera
   evaluación las reglas esperan a que el consumidor alcance la ventana: un poll vacío (100 ms sin
   mensajes) o un poll cuyo mensaje más nuevo tiene < 2 s de antigüedad; tope 10 s. Con varias
   particiones, una que aún repite historia mientras otra ya está al día puede resolver por error
   una alerta persistida (se vuelve a disparar al llegar sus datos).
5. **Canales.** `[alerts.channels.<nombre>]` en `duct.toml` con `type` (módulo `alert.channel`) y
   sus claves; `<clave>_env = "VAR"` pone el valor de la variable de entorno en `<clave>`
   (secretos fuera de `duct.toml`). La regla referencia `tipo:nombre`; se valida al arrancar
   (`duct alerts check`). Todos los canales de una transición van en paralelo, con 3 intentos
   (0, 200 ms, 1 s); transiciones de la misma alerta se entregan en orden.
6. **Registro.** Cada transición, con su entrega (`channels`, `delivered`), en
   `bronze.duct_alerts`; las alertas activas en `duct.duct_alert_state` (migración 5). Los
   errores de canal se guardan sin URLs (los webhooks de Slack son secretos).

## Semántica de entrega
Se avisa **antes** de guardar el estado: un crash puede repetir un aviso (al reiniciar, la
alerta activa se carga del estado y no se repite mientras siga disparada; si el crash cayó entre
el aviso y el guardado, se repite una vez), pero no perderlo. Un canal caído no bloquea a los
demás ni al registro: queda `delivered = false` con el error.

## Límites conocidos
Sin `FOR`/histéresis (una lectura ruidosa alterna firing/resolved: usar una ventana en la regla),
sin recordatorios periódicos, sin silencios. Varias instancias comparten el estado de
`duct.duct_alert_state` sin coordinación: no hay failover activo/activo probado.

Tests: `core/alerts/alerts_test.go` (reglas, máquina de estados), `cmd/duct/alerts_test.go`
(Runner con consumidor falso), `test/integration/stream_kafka_test.go` (Kafka real).
