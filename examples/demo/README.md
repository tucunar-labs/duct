# Demo de Duct: los cinco pasos de v0.1

Proyecto de ejemplo. Lo ejecuta como test e2e `test/e2e/demo_test.go`
(`just e2e`); también puedes seguirlo a mano con el binario `duct`.

```sh
cp -r examples/demo /tmp/demo && cd /tmp/demo
duct init .                      # 1. catálogo SQLite + ./lake (sobre esta carpeta)
duct ingest                      # 2. data/ventas.csv (y clientes por ODBC) -> bronze
duct run                         # 3. Bronze -> Silver -> Gold con EXPECT
./setup.sh                       # 4. rol analistas, usuario ana, filtro y máscara
duct serve                       #    pg, http y web (127.0.0.1:8484)
psql "host=127.0.0.1 port=<puerto pg> user=ana dbname=duct" -c "SELECT * FROM ventas"
# 5. abre la web, entra como ana y mira /catalog
```

1. **init**: crea `duct.toml`, `./lake` y el catálogo. No toca los archivos del ejemplo.
2. **ingest**: `ingest/ventas.yaml` (CSV) y `ingest/clientes.yaml` (Postgres por
   ODBC; necesita unixODBC, psqlODBC y las variables `PG_ODBC_DRIVER`, `PGPORT`,
   `PGPW`). Sin Postgres, borra `ingest/clientes.yaml` y `pipelines/clientes.sql`.
3. **run**: `silver.ventas` descarta 2 filas (`monto > 0`, `DROP ROW`). Para ver
   `FAIL UPDATE`, agrega una venta grande a `data/ventas.csv`
   (`7,2026-01-11,AR,22.222.222-2,5000`), vuelve a `duct ingest` y `duct run`:
   `gold.ventas_por_region` falla (total >= 1000) y conserva su versión anterior;
   `gold.ventas` sí se actualiza.
4. **serve**: `ana` ve solo `ventas` con `region = 'CL'` y el RUT enmascarado; las
   tablas base (`gold.ventas`, `bronze.*`) le están negadas, por pg y por HTTP.
5. **web**: login y `/catalog` muestran solo lo que ve el rol.
