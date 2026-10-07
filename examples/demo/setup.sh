#!/usr/bin/env bash
# Paso 4 (preparación): rol, usuario, permiso, filtro por fila y máscara.
# Corre después de `duct run` (las vistas por rol se generan sobre gold.ventas).
# Uso: ./setup.sh   (DUCT=ruta/al/binario para no usar el del PATH)
set -euo pipefail
cd "$(dirname "$0")"
duct="${DUCT:-duct}"
pw="${DEMO_PASSWORD:-demo-pw-123}"

"$duct" role create analistas
"$duct" grant select on gold.ventas to analistas
"$duct" policy row-filter gold.ventas --role analistas --where "region = 'CL'"
"$duct" policy mask gold.ventas.rut --role analistas --expr "'***'"
"$duct" user create ana --role analistas --password "$pw"
