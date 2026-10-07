# 0009. Alertas

Estado: aceptada (2026-10-03)

## Decisión (lo mínimo posible)
- `duct alerts` dentro del binario: consume Kafka (consumer groups para escalar), guarda los
  últimos N minutos en DuckDB en memoria y evalúa reglas SQL (`alerts/*.sql` con
  `EVERY 200ms`) cada ~100–200 ms. Umbrales, ventanas, correlación entre sensores, "sensor mudo".
- Estado por alerta (disparada/resuelta), dedup por regla + clave; avisa también al resolverse.
- Registra cada alerta en Bronze.
- Canales nativos: **ntfy** (push Android/iOS + email; iOS usa ntfy.sh como relay a APNs),
  **webhook** (POST; Slack/Teams/Discord), **WhatsApp** (Cloud API de Meta, plantillas
  *utility*; credenciales por instalación), **PagerDuty** (Events API v2, `dedup_key`).

## Futuro
- App propia con FCM/APNs (Gorush) y Critical Alerts de iOS. Flink si aparece CEP complejo.

## Descartados
- Shoutrrr (no aporta sobre ntfy), whatsmeow (viola ToS de Meta), Proton/Flink/RisingWave/
  Feldera como motor extra.

## Implementado (2026-10-07)
- `duct alerts` (y `[alerts]` en `duct.toml` dentro de `duct serve`), código en `core/alerts`
  (ver su AGENTS.md). Consumer group propio sobre los topics de los streams con `window:`;
  no confirma offsets (cada arranque repite la ventana desde Kafka). Los últimos N minutos
  viven en un DuckDB en memoria aparte (`engine-duckdb` con `"memory": true`), una tabla por
  stream.
- Reglas `alerts/*.sql` con directivas de cabecera `EVERY`, `KEY`, `SEVERITY`, `CHANNELS`,
  `MESSAGE`; una evaluación por regla cada `EVERY` (mínimo 10 ms). Estado por (regla, clave):
  `firing` → `resolved`, con dedup; se avisa en ambas transiciones. Una evaluación con error no
  cambia el estado. `duct alerts check` valida reglas y canales.
- Canales por instancia, `[alerts.channels.<nombre>]` con `type` y `<clave>_env` para los
  secretos: `ntfy`, `webhook`, `pagerduty` (Events v2, `dedup_key = regla:clave`) y `whatsapp`
  (Cloud API, plantilla *utility*; **solo probado contra `httptest`**, sin credenciales de
  Meta). Reintentos 0/200 ms/1 s por canal, en paralelo; en orden por alerta.
- Cada transición queda en `bronze.duct_alerts` (con `delivered` y el resumen de entrega, sin
  URLs) y las activas en `duct.duct_alert_state` (migración 5): al reiniciar no se repiten
  avisos de alertas que siguen disparadas ni se olvidan las que se resolvieron.
- Medido (Kafka real, regla cada 200 ms): umbral → webhook/ntfy/PagerDuty/WhatsApp ~115 ms
  desde que se produce el mensaje; resolución ~195 ms; "sensor mudo" con umbral de 2 s avisa a
  los 2,0 s. Entrega: se notifica antes de guardar el estado, así un crash puede repetir un aviso
  pero no perderlo.
- Límites: sin histéresis (`FOR`), recordatorios ni silencios; escalar con varias instancias
  exige particionar por la clave de la alerta y no está probado activo/activo.
