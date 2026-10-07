# examples/

Proyectos de ejemplo completos: ingesta, pipelines Bronze/Silver/Gold con `EXPECT`,
contrato ODCS, capa semántica Ossie, dashboards y un job. Cada uno tiene su README.

| Proyecto | Datos | Tamaño | Offline |
|---|---|---|---|
| [`demo/`](demo/) | Ventas de juguete, gobierno (rol, filtro, máscara) | KB | sí |
| [`penguins/`](penguins/) | Pingüinos de Palmer (CC0) | 15 KB, versionado | sí |
| [`tpch/`](tpch/) | TPC-H generado con DuckDB (sf=0,1) | ~25 MB generados | sí (descarga la extensión `tpch` una vez) |
| [`nyc-taxi/`](nyc-taxi/) | Taxis amarillos de Nueva York, enero 2024 | ~50 MB, `download.sh` | no |

`demo`, `penguins` y `tpch` los ejecuta `just e2e` (`test/e2e`); `nyc-taxi` solo se
valida con lint en CI (no se descarga nada). Titanic no se incluye: sus copias
públicas no declaran una licencia clara.

Receta común (desde una copia, para no ensuciar el repo):

```sh
cp -r examples/penguins /tmp/penguins && cd /tmp/penguins
duct init . && duct ingest && duct run && ./setup.sh
duct contract test && duct semantic validate && duct dashboard lint
duct serve        # web en http://127.0.0.1:8484, entra como ana
```
