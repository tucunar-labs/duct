# Taxis amarillos de Nueva York

Cada viaje de taxi amarillo registrado por la NYC Taxi and Limousine Commission (TLC):
horas de salida y llegada, zonas, distancia, tarifa, propina y forma de pago. Enero de
2024 son 2,96 millones de filas (~50 MB en Parquet). Con datos reales vienen errores
reales: viajes con fecha de 2002, tarifas negativas (reembolsos), llegadas antes de
la salida. Silver los descarta con `EXPECT` y los cuenta en cada `duct run`.

## Origen y licencia

- NYC TLC Trip Record Data: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>,
  Parquet en `https://d37ci6vzurychx.cloudfront.net/trip-data/` y el catálogo de zonas
  (`taxi_zone_lookup.csv`) en `.../misc/`.
- Datos abiertos de la Ciudad de Nueva York, publicados bajo los términos de uso de
  NYC Open Data (<https://opendata.cityofnewyork.us/overview/#termsofuse>): uso libre,
  sin garantías. La TLC los recopila de los proveedores de tecnología de los taxis.
- No se versionan (`data/` está en `.gitignore`); `download.sh` los baja.

## Qué hay

| Capa | Archivo | Qué hace |
|---|---|---|
| Datos | `download.sh` | baja `yellow_tripdata_2024-01.parquet` y `taxi_zone_lookup.csv` a `data/` (`MONTHS="2024-01 2024-02"` para más) |
| Ingesta | `ingest/trips.yaml`, `ingest/zones.yaml` | Parquet (glob mensual) y CSV a `bronze.trips` y `bronze.zones` |
| Silver | `pipelines/silver.sql` | nombres limpios; descarta ~38 000 viajes: `fare_amount >= 0`, `total_amount >= 0`, propina >= 0, distancia < 500 mi, llegada >= salida, año 2024 |
| Gold | `pipelines/gold.sql` | `gold.trips_by_hour` (día x hora x borough x pago, con sumas) y `gold.top_zones` |
| Contrato | `contracts/trips_by_hour.odcs.yaml` | llave única, sin sumas negativas, horas 0-23, fechas en 2024 |
| Semántica | `semantic/taxi.ossie.yaml` | `trips`, `revenue`, `avg_fare`, `avg_distance`, `avg_tip`; dimensiones `weekday`, `hour`, `borough`, `payment_type`, `date` |
| Dashboard | `dashboards/taxis.yaml` | KPI, mapa de calor día x hora, barras por borough, forma de pago, serie diaria, zonas |
| Job | `jobs/monthly.yaml` | `download.sh`, ingesta y pipelines, el día 5 de cada mes |

Notas: las propinas en efectivo no se registran, por eso `avg_tip` es baja salvo en
tarjeta. Los promedios se calculan como suma / viajes en la capa semántica, así
cualquier combinación de filtros da el promedio correcto. `fecha_en_2024` fija el año:
para otros años cambia esa regla y la del contrato.

## Cómo correrlo

```sh
cp -r examples/nyc-taxi /tmp/taxi && cd /tmp/taxi
./download.sh            # ~50 MB
duct init .
duct ingest              # ~3 millones de filas
duct run                 # Silver descarta ~37 600 viajes; Gold con contrato
duct contract test
duct semantic validate
duct dashboard lint
./setup.sh               # rol analistas + usuario ana (clave demo-pw-123)
duct jobs run monthly    # el job a mano
duct serve               # http://127.0.0.1:8484/dashboards, entra como ana
```

En CI solo se hace lint de los archivos (sin descargar nada).
