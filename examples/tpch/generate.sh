#!/usr/bin/env bash
# Genera TPC-H con la extensión `tpch` de DuckDB y deja un Parquet por tabla en data/.
# La primera vez DuckDB descarga la extensión (necesita red una sola vez).
# Uso: ./generate.sh   (TPCH_SF=0.1 por defecto; DUCT=ruta/al/binario)
set -euo pipefail
cd "$(dirname "$0")"
duct="${DUCT:-duct}"
sf="${TPCH_SF:-0.1}"
db="$(mktemp -u "${TMPDIR:-/tmp}/tpch-XXXXXX.duckdb")"
trap 'rm -f "$db" "$db.wal"' EXIT
mkdir -p data

copies=""
for t in region nation customer orders lineitem; do
  copies+="COPY (SELECT * FROM tp.main.$t) TO 'data/$t.parquet' (FORMAT parquet, COMPRESSION zstd);"
done

# dbgen solo escribe en archivos .duckdb, por eso el ATTACH temporal.
"$duct" query "INSTALL tpch; LOAD tpch; ATTACH '$db' AS tp; CALL dbgen(sf=$sf, catalog='tp'); $copies" >/dev/null
echo "TPC-H sf=$sf en data/: $(ls data | tr '\n' ' ')"
