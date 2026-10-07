#!/usr/bin/env bash
# Descarga los datos públicos de la TLC de Nueva York a data/ (no se versionan).
#   - Yellow Taxi trip records, un Parquet por mes (~50 MB el de enero 2024).
#   - Taxi zone lookup (CSV, 265 zonas con su borough).
# Uso: ./download.sh               (MONTHS="2024-01" por defecto)
#      MONTHS="2024-01 2024-02" ./download.sh
# Idempotente: no vuelve a bajar lo que ya está. Los meses deben ser de 2024
# (el pipeline descarta viajes de otros años; ver pipelines/silver.sql).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data
base="https://d37ci6vzurychx.cloudfront.net"

fetch() { # url destino
  [ -s "$2" ] && { echo "ya existe: $2"; return; }
  echo "descargando $1"
  curl --fail --silent --show-error --location --retry 3 --output "$2.part" "$1"
  mv "$2.part" "$2"
}

for m in ${MONTHS:-2024-01}; do
  fetch "$base/trip-data/yellow_tripdata_$m.parquet" "data/yellow_tripdata_$m.parquet"
done
fetch "$base/misc/taxi_zone_lookup.csv" data/taxi_zone_lookup.csv
