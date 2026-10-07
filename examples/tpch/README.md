# TPC-H

El benchmark de ventas al por mayor TPC-H: clientes, pedidos y líneas de pedido de
una empresa ficticia, 1992-1998. Los datos son **sintéticos** y se generan localmente
con la extensión `tpch` de DuckDB; no hay nada que descargar salvo la extensión (una
sola vez). Con `sf=0,1`: 15 000 clientes, 150 000 pedidos y 600 572 líneas.

## Origen y licencia

- Generador `dbgen` del Transaction Processing Performance Council (TPC), empaquetado
  en la extensión `tpch` de DuckDB (MIT): <https://duckdb.org/docs/extensions/tpch>.
  La especificación y el generador se rigen por la *TPC End User License Agreement*
  (<https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp>).
- Aquí no se publican datos ni resultados de benchmark: se usan solo como datos de
  ejemplo, y las cifras de este ejemplo no son resultados TPC-H oficiales. "TPC-H" es
  marca del TPC.
- Los datos generados (`data/*.parquet`) no se versionan.

## Qué hay

| Capa | Archivo | Qué hace |
|---|---|---|
| Datos | `generate.sh` | `CALL dbgen(sf=0.1)` sobre un `.duckdb` temporal y `COPY` a `data/*.parquet` (`TPCH_SF` cambia la escala) |
| Ingesta | `ingest/*.yaml` | cinco Parquet a `bronze.{region,nation,customer,orders,lineitem}` |
| Silver | `pipelines/silver.sql` | nombres limpios y `EXPECT ... DROP ROW` (segmento válido, descuento 0-10 %, precios positivos) |
| Gold | `pipelines/gold.sql` | `gold.revenue_by_nation` (mes x segmento x región x nación, ingreso neto = precio x (1 - descuento)) y `gold.top_customers` (top 100) |
| Contrato | `contracts/revenue_by_nation.odcs.yaml` | llave única, ingresos positivos, cinco regiones, frescura |
| Semántica | `semantic/tpch.ossie.yaml` | métricas `revenue`, `orders`, `lines`, `revenue_per_order`, `top_customer_revenue`; dimensiones `year`, `month`, `segment`, `region`, `nation`, `customer` |
| Dashboards | `dashboards/ingresos.yaml` | tendencia de ingresos: KPI, líneas por año y región, área mensual, barras por nación |
| | `dashboards/participacion.yaml` | participación por región y segmento, mapa de calor región x nación, mejores clientes, filtro de año |
| Job | `jobs/refresh.yaml` | ingesta + pipelines, lunes 03:00 (America/Santiago) |

## Cómo correrlo

```sh
cp -r examples/tpch /tmp/tpch && cd /tmp/tpch
duct init .
./generate.sh            # ~2 s; necesita red solo la primera vez (INSTALL tpch)
duct ingest
duct run
duct contract test
duct semantic validate
duct dashboard lint
./setup.sh               # rol analistas + usuario ana (clave demo-pw-123)
duct semantic query --role analistas --dims region --measures revenue,orders
duct serve               # http://127.0.0.1:8484/dashboards, entra como ana
```
