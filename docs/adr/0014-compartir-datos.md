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

## Implementado (2026-10-07)

Código: `core/sharing` (modelo, copias, almacenamiento), `modules/sharing-server` (REST),
`cmd/duct/share.go` (CLI), migración 7. Pruebas: `TestE2EShare` (cliente oficial `delta-sharing`
de Python 1.4), `TestShareS3RustFS` (RustFS), `modules/sharing-server/server_test.go` (protocolo).

**Modelo y CLI.** `duct share create|add|remove|drop|grant|revoke|list|refresh|gc` y
`duct recipient create|list|token|revoke|drop`. Un destinatario es el rol `share_<nombre>`, un
usuario del mismo nombre (contraseña aleatoria descartada: solo entra con token) y un token
`dct_…` (por defecto 90 días; `--ttl 0` sin vencimiento). `recipient create` escribe el perfil
`.share` (`shareCredentialsVersion`, `endpoint`, `bearerToken`, `expirationTime`; modo 600; el
token no se imprime). `share grant … --row-filter … --mask col=expr [--table t]` guarda la
política en las tablas de gobierno del **rol** del destinatario, por relación (misma validación
que `duct policy`: parser de DuckDB, lista de denegación, enlace contra el lake). Consecuencia:
la política es del destinatario y la comparten los shares suyos que lleven la misma tabla.
`share add --partition-filter` es un predicado fijo extra, igual para todos los destinatarios
(se evalúa sobre la vista del rol, es decir, sobre valores ya enmascarados).

**Fase 1 (por consulta).** El mismo token sirve en `serve-http` (`Authorization: Bearer`) y,
como usuario `share_<nombre>`, en el protocolo Postgres: ve solo sus vistas `role_share_<n>`.
Revocar el token o el grant cierra el acceso. Todo queda en `duct_audit`.

**Fase 2 (servidor).** Protocolo `sharing` de `duct serve`
(`[serve.sharing]`; arranca solo si esa tabla existe o con `--protocols sharing`; por defecto
`127.0.0.1:8124`, no loopback exige TLS). Endpoint `…/delta-sharing`:

| Endpoint | Soporte |
|---|---|
| `GET /shares`, `/shares/{s}`, `/shares/{s}/schemas`, `…/schemas/{sch}/tables`, `…/all-tables` | sí, con `maxResults`/`pageToken`; solo lo concedido al token |
| `GET`/`HEAD …/tables/{t}/version` | sí (`Delta-Table-Version`); `startingTimestamp` se ignora |
| `GET …/metadata` | sí (NDJSON: `protocol`, `metaData`) |
| `POST …/query` | sí (`protocol`, `metaData`, `file` con URL firmada, `size`, `stats.numRecords`); `limitHint` recorta archivos; `predicateHints` se ignoran |
| `version` distinto del vigente, `timestamp`, `GET …/changes` (CDF) | 400 / 501: solo la versión actual |
| Formato | `parquet` (`Delta-Sharing-Capabilities: responseformat=parquet`); sin deletion vectors ni CDF |

**Copias.** Por (share, destinatario, tabla) se materializa un Parquet **compactado** (zstd,
archivos de hasta 2 M de filas) con lo que devuelve la vista del rol, leída a través del
`Handler` gobernado (filtros, máscaras y auditoría de gobierno; no hay otro camino a los
datos). Capa: `shares/<share>/<destinatario>/<tabla>/v<snapshot>/part-NNNNN.parquet`.
- **Refresco perezoso por snapshot**: cada consulta compara el snapshot del lake con el de la
  copia; si cambió, calcula una huella (esquema + conteo + suma de `hash(fila)`) de lo que ve el
  rol y **solo reescribe si cambió** (la versión del protocolo es un contador por copia, así que
  un cambio en otra tabla no la mueve). Elegido sobre un gancho tras `duct run` porque funciona
  igual con `run`, `ingest`, `stream` o escrituras manuales y no depende de quién escribe.
  Alternativas: `duct share refresh [share]` (cron o CI; las tareas de `jobs/` no sirven, no
  llevan credenciales del lake) y `refresh_interval = "5m"` dentro de `duct serve`.
- **Ubicación fuera del `data_path`**: `./shares/` junto a `lake/`, o `s3://bucket/<prefijo>.shares`.
  Obligatorio: `ducklake_delete_orphaned_files` (`duct gc`) borra todo archivo desconocido bajo
  el `data_path` (comprobado), y `Init` rechaza una ubicación que se solape con el lake.
  `.gitignore` de `duct init` incluye `shares/`.
- **GC**: tras escribir una versión se borran las anteriores con más de 1 h (`grace`, para que
  una descarga en curso termine); `duct share gc` barre huérfanos; `share revoke|remove|drop` y
  `recipient drop` borran las copias al instante.
- **URLs de archivo**: ~15 min (`url_ttl`). Almacenamiento local: `…/delta-sharing/_files/<clave>?exp&sig`
  servido por el propio proceso, HMAC-SHA256 sobre clave+expiración (clave en `.duct/sharing.key`,
  modo 600), solo `.parquet`, sin rutas no canónicas, con `Range`. S3: URL prefirmada de
  `GetObject` (SigV4) sobre el prefijo de copias (`s3_public_endpoint` si el destinatario ve
  el almacén por otra dirección). Nunca se entrega una URL de un archivo base.

**Seguridad.** Token inválido, revocado o vencido: 401 (se comprueba en cada llamada; no hay
sesión). Token válido de alguien que no es destinatario (`share_<n>` con usuario del mismo
nombre): 403. Share o tabla no concedidos: 404 igual que inexistentes. Cada llamada se audita
en `duct_audit` con protocolo `sharing` (usuario `share_<n>`; descargas locales también;
intentos sin identidad como `-`). Límite conocido: una URL ya emitida sigue valiendo hasta
vencer (≤ 15 min) aunque se revoque al destinatario; el tipo de dato sin equivalente Delta
(tiempo, UUID, listas, structs…) se castea a texto y `UBIGINT` a `DECIMAL(20,0)`; las
columnas `TIMESTAMP` sin zona se declaran `timestamp`. Pendiente: Iceberg REST, clean rooms,
CDF, `predicateHints` (poda), paginación de archivos, límite de tasa de intentos fallidos.
