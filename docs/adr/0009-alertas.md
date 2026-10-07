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
- Límites (resueltos abajo): histéresis, recordatorios, silencios y activo/activo.

## Implementado (2026-10-07): histéresis, recordatorios, silencios y activo/activo
- **Directivas** `-- FOR 30s` (la condición debe sostenerse ese tiempo antes de disparar; un
  parpadeo más corto no dispara nada ni se registra; `firing_since` es el inicio de la condición),
  `-- RESOLVE_FOR 1m` (simétrico: la alerta sigue disparada mientras la ausencia dure menos) y
  `-- REPEAT 1h` (mientras siga disparada se avisa de nuevo cada periodo: el canal recibe
  `firing` con la etiqueta `reminder=true` y el mensaje con prefijo `[reminder]`; en Bronze el
  estado es `reminder`). Los relojes de espera viven en memoria: un reinicio los reinicia. Evidencia:
  `core/alerts` (`TestTracker*`), `TestAlertsHysteresisAndReminders`.
- **Silencios:** `duct alerts silence <regla> [--key k] --for 2h --reason "..."`, `silences`,
  `unsilence <id>`, guardados en `duct.duct_alert_silences` (migración 11). El notificador los
  consulta al entregar: un silencio cubre firing, recordatorios y resolved de la regla (o de una
  clave), no envía a los canales, avanza el estado igual y deja la transición en Bronze con
  `channels = "silenced #id until … (motivo)"`. Se ven en `duct alerts status` (alertas activas
  con `SILENCED`) y `silences`; expiran solos. Un silencio que vence con la alerta aún
  disparada no vuelve a avisar hasta el siguiente recordatorio. Evidencia: `TestAlertsSilences`.
- **Activo/activo.** Varias instancias `duct alerts` pueden correr a la vez, en el mismo grupo
  (Kafka reparte las particiones) o en grupos distintos (cada una ve todo). Un *lease* por
  (regla, clave) en `duct.duct_alert_state` decide quién notifica cada transición
  (`core/alerts/coord.go`, con `txretry`, sin ningún servicio nuevo): disparar = `INSERT` con la PK
  (quien lo inserta notifica, el otro adopta la alerta sin avisar); el aviso se marca hecho con
  `FiringDone`; si el titular muere antes de avisar, el lease vence y otra instancia termina el
  trabajo; los recordatorios se reclaman moviendo `notified_at` con un token de lectura posterior;
  resolver = pasar a `resolving` con token y borrar al terminar. Una instancia solo resuelve lo que
  nadie más confirmó viendo la alerta disparada *después* de su última lectura (si una partición
  se mudó a otra instancia, la nueva dueña la sigue confirmando y la vieja la suelta sin avisar);
  las alertas que nadie confirma en 4 leases (mínimo 60 s: más que un rebalanceo de Kafka) se resuelven por barrido. Una lápida corta
  (`duct_alert_resolved`) evita que una instancia con datos atrasados anuncie de nuevo lo que otra
  acaba de resolver. Si la coordinación falla se notifica igual (mejor un duplicado que perder una
  alerta). Config: `[alerts] instance` / `DUCT_ALERTS_INSTANCE` (nombre; por defecto aleatorio) y
  `lease_ttl` (15 s; nunca menos de 3 evaluaciones de la regla): un titular muerto se reemplaza
  tras ese tiempo. Las columnas nuevas de `duct_alert_state` guardan los instantes en `BIGINT`
  (microsegundos): el scanner de SQLite de DuckDB no sabe hacer `UPDATE` de una columna
  `TIMESTAMP` agregada con `ALTER TABLE`. Evidencia: `TestAlertsActiveActive` (cmd/duct: dos
  instancias, cada transición una vez, recordatorios sin doble envío, la sobreviviente resuelve
  tras detener a la otra) y `test/integration` `TestAlertsActiveActive` (Postgres + Kafka en Podman,
  mismo grupo con 2 particiones, `kill -9` de una instancia). Límite: tras reiniciar una instancia
  con nombre aleatorio, una alerta que se despejó durante la caída se resuelve recién cuando su
  última confirmación tiene más de un lease (o por barrido de 60 s); fijar `instance` lo evita.

