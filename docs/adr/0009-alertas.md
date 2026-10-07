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
