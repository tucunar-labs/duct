# 0014. Compartir datos

Estado: aceptada (2026-10-04)

## Decisión: protocolo OpenSharing (ex-Delta Sharing, LF, Apache-2.0)
- Fase 1, por consulta (~300–500 líneas Go): el destinatario es un rol de `duct serve` con
  token. Gobierno por vistas por rol; revocar = borrar token; auditoría = log de consultas.
- Fase 2, servidor OpenSharing en Go (~1.500–2.500 líneas): copia materializada, filtrada y
  enmascarada por share/destinatario en `shares/<share>/<dest>/`, compactada (sin delete
  files, inlining ni cifrado), refrescada por snapshot; URLs firmadas ~15 min; formato
  `parquet`, sin deletion vectors ni CDF al inicio.
- Diferido: Iceberg REST de OpenSharing, clean rooms.
