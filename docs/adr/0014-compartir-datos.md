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
| `GET`/`HEAD …/tables/{t}/version` | sí (`Delta-Table-Version`); `startingTimestamp` solo con historial (ver 2026-10-18) |
| `GET …/metadata` | sí (NDJSON: `protocol`, `metaData`) |
| `POST …/query` | sí (`protocol`, `metaData`, `file` con URL firmada, `size`, `stats`); `limitHint` recorta archivos; `jsonPredicateHints`/`predicateHints` podan por estadísticas (2026-10-18) |
| `version`, `timestamp`, `startingVersion`, `GET …/changes` (CDF) | solo en tablas con historial (2026-10-18); sin historial, 400 |
| Formato | `parquet` (`Delta-Sharing-Capabilities: responseformat=parquet`); sin deletion vectors |

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
columnas `TIMESTAMP` sin zona se declaran `timestamp`. Pendiente: Iceberg REST (diseño abajo), clean rooms,
paginación de archivos, límite de tasa de intentos fallidos.

## Implementado: historial, CDF y poda (2026-10-18)

Código: `core/sharing/history.go` (versiones, consulta del feed), `predicate.go` y `stats.go` (poda),
`modules/sharing-server/api.go`, migración 13. Pruebas: `TestTimeTravelAndChangeFeed` y
`TestPredicateHints` (`modules/sharing-server`), `predicate_test.go`, `TestE2EShareHistory` (cliente
oficial: `load_table_changes_as_pandas`, `version=`, `timestamp=`), `TestShareHistoryS3RustFS`.

**Activación.** Por tabla: `duct share add <share> <tabla> --history` o `duct share history <share>
<tabla> [--off]`; `duct share list` marca `[history]`. Retención `[serve.sharing] keep_versions`
(10 por defecto; `duct share refresh|gc` la leen igual). Sin historial todo sigue como antes (solo la
versión vigente; `version`, `timestamp`, `startingVersion` y `/changes` responden 400 con el motivo).

**Decisión: versiones materializadas, no re-filtrado.** Se eligió conservar N versiones del *linaje
de copias* del destinatario en lugar de derivarlas de los snapshots de la tabla base y refiltrarlas por
el rol. Razón: una vista del rol no se puede leer `AT (VERSION => n)` (la política viaja con la vista
vigente), así que reconstruir el pasado exigiría reaplicar la política *actual* a datos viejos, lo que
filtra lo que el destinatario no debía ver si la política cambió. Cada versión retenida es lo que el
destinatario vio en ese momento, con la política de entonces; no hay camino a datos sin filtrar.
Costo: espacio (N copias compactadas) y granularidad: hay una versión por refresco observado
(consulta de un cliente, `duct share refresh` o `refresh_interval`), no por snapshot del lake; varios
snapshots entre dos refrescos son una sola versión. La marca de tiempo de una versión es la del
snapshot del lake con que se materializó (no la hora de la consulta).
- Estado: `duct_share_versions` (archivos, archivos de cambios, snapshot, `ts_ms`, tipo, hash de
  política) y `duct_share_history`. El GC trata como vivos los archivos de todas las versiones
  retenidas y de sus feeds; los de versiones podadas se borran pasada la gracia (`grace`).
- `version=n` y `timestamp=t` (`/query`): la versión retenida `n`, o la última creada en o antes de
  `t` (400 si es anterior a la primera retenida). `GET …/version?startingTimestamp=t`: la primera
  creada en o después de `t` (400 si es posterior a la última). Las marcas aceptan ISO 8601 UTC
  (con o sin fracción). Una versión podada o inexistente es 400 con el rango retenido.

**CDF (`GET …/changes`, `POST …/query` con `startingVersion`).** Parámetros `startingVersion`,
`endingVersion`, `startingTimestamp`, `endingTimestamp` (ambos extremos inclusivos; obligatorio uno de
los de inicio; `includeHistoricalMetadata` se ignora: un cambio de esquema es un hueco, ver abajo).
Respuesta `parquet`: `protocol`, `metaData` (`enableChangeDataFeed`) y, por versión, acciones `cdf`
(archivo Parquet con las columnas de la tabla más `_change_type`; `_commit_version` y
`_commit_timestamp` van en la acción `version`/`timestamp`, como en Delta y como los lee el
cliente) o, para la primera versión de una copia, `add` (todo inserciones). URLs firmadas igual que
los datos (local o S3 prefirmado).
- **Cómo se calcula.** Al escribir una versión nueva (no en la lectura), `table_changes()` de
  DuckLake entre el último snapshot verificado y el actual da los cambios de la tabla base. Se
  **netean por fila** (`rowid`, estado primero y último del rango) y se aplica la política del
  destinatario a ambos estados exactamente como la vista del rol (filtro de filas sobre valores
  base, máscaras, y el filtro fijo de la tabla sobre valores ya enmascarados). Es el único lugar que
  lee la tabla base fuera del `Handler`; lo cubre `TestTimeTravelAndChangeFeed` (versión *n* + feed =
  versión *n+1*) y la prueba de consistencia en línea (ver abajo).
- **Semántica para el destinatario.** Fila que *entra* a su vista (insertada, o actualizada hacia
  dentro del filtro) = `insert`; fila que *sale* (borrada, o actualizada hacia fuera) = `delete` con
  los valores que él vio por última vez (preimagen enmascarada); dentro antes y después =
  `update_preimage`/`update_postimage`, salvo que los valores visibles sean iguales (un cambio solo en
  una columna enmascarada no se informa); nunca dentro = no aparece (no se filtra ni su existencia).
  Insertar y borrar dentro de una misma versión no deja rastro.
- **Huecos (nunca un feed incorrecto).** Una versión no tiene feed (`gap`, 400 al pedirla, con el
  motivo) si cambió la política del destinatario o el esquema respecto de la versión anterior (hash
  de política en cada versión), si no se pudieron leer los cambios, si la suma no cuadra
  (`filas anteriores + inserts − deletes ≠ filas nuevas`, comprobación en línea tras escribir) o si
  la tabla cambió mientras se copiaba (se reintenta hasta 3 veces a un snapshot más nuevo; luego
  hueco). Con el historial recién activado, la versión vigente es `base` y el feed empieza en la
  siguiente. Una columna llamada `rowid`, `snapshot_id`, `change_type` o `_s`… (nombres internos de
  la consulta) deja la versión sin feed.
- Límite conocido: la marca `_commit_timestamp` es una por versión (la de su snapshot), no por fila.

**Poda (`jsonPredicateHints`, `predicateHints`).** Cada archivo Parquet lleva estadísticas Delta
(`numRecords`, `minValues`, `maxValues`, `nullCount`) calculadas al escribirlo sobre los valores ya
filtrados y enmascarados (enteros, `double`, texto hasta 64 bytes y fechas; `float32`, decimales y
timestamps no llevan min/max). `jsonPredicateHints` (cadena JSON o el árbol mismo) acepta `and`, `or`,
`not`, `isNull`, `equal`, `lessThan(OrEqual)`, `greaterThan(OrEqual)`; el `predicateHints` heredado,
`col op literal`, `literal op col` y `col IS [NOT] NULL`. Un archivo se omite solo si las
estadísticas prueban que ninguna fila cumple (lógica de tres valores: `NOT` de «ninguna cumple» no
poda, por los nulos); todo lo demás —operador desconocido, columna sin estadísticas, tipo que no
encaja, JSON malo, exponentes absurdos— conserva el archivo (las pistas son opcionales). La poda va
antes de `limitHint`. La copia no tiene columnas de partición, así que no hay poda por partición:
solo por estadísticas, y los archivos se cierran por lotes de ~2 048 filas (`rows_per_file`), así que
la poda rinde en tablas grandes ordenadas de forma natural.

**Iceberg REST (solo lectura): evaluado, no implementado.** OpenSharing especifica el catálogo REST de
Iceberg (`/v1/config?warehouse=<share>`, `namespaces`, `tables`, `loadTable`, `metrics`; spec
`TABLES.md`). Esfuerzo estimado: 1 500–2 500 líneas y una dependencia nueva, más una decisión de
credenciales. Diseño para cuando se haga: (1) por versión de copia, metadatos Iceberg v2 generados
sobre los mismos Parquet (`metadata.json`, lista de manifiestos y manifiestos en Avro con
estadísticas de columna; sin IDs de campo en los archivos, con `schema.name-mapping.default`), un
snapshot Iceberg por versión retenida (el historial da el time travel); (2) `loadTable` devuelve la
ubicación del metadata y la configuración de E/S del cliente; (3) un `iceberg` en `format` del
listado de tablas. Lo que lo bloquea: los clientes Iceberg leen con FileIO y credenciales vendidas
(STS en S3), no con URLs prefirmadas por archivo; RustFS/Garage no ofrecen STS, y servir los archivos
por el propio `duct serve` exigiría un FileIO por HTTP que los clientes estándar no traen. Sin
STS el beneficio no justifica la superficie; se retoma si un cliente lo pide y hay S3 real.
