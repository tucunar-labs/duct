#!/usr/bin/env bash
# Gobierno: rol "analistas" con lectura de Gold y un usuario para entrar a la web.
# Corre después de `duct run`. Uso: ./setup.sh   (DUCT=ruta/al/binario, DEMO_PASSWORD=...)
set -euo pipefail
cd "$(dirname "$0")"
duct="${DUCT:-duct}"
pw="${DEMO_PASSWORD:-demo-pw-123}"

"$duct" sql "CREATE ROLE analistas"
"$duct" sql "GRANT SELECT ON TABLE gold.revenue_by_nation TO ROLE analistas"
"$duct" sql "GRANT SELECT ON TABLE gold.top_customers TO ROLE analistas"
"$duct" user create ana --role analistas --password "$pw"
