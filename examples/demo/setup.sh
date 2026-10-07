#!/usr/bin/env bash
# Paso 4 (preparación): rol, usuario, permiso, filtro por fila y máscara.
# Corre después de `duct run` (las vistas por rol se generan sobre gold.ventas).
# Uso: ./setup.sh   (DUCT=ruta/al/binario para no usar el del PATH)
set -euo pipefail
cd "$(dirname "$0")"
duct="${DUCT:-duct}"
pw="${DEMO_PASSWORD:-demo-pw-123}"

"$duct" sql "CREATE ROLE analistas"
"$duct" sql "GRANT SELECT ON TABLE gold.ventas TO ROLE analistas"
"$duct" sql "ALTER TABLE gold.ventas SET ROW FILTER region = 'CL' FOR ROLE analistas"
"$duct" sql "ALTER TABLE gold.ventas ALTER COLUMN rut SET MASK '***' FOR ROLE analistas"
"$duct" user create ana --role analistas --password "$pw"
