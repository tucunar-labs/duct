-- Bronze -> Silver: descarta ventas con monto no positivo.
CREATE MATERIALIZED VIEW silver.ventas (
  CONSTRAINT monto_positivo EXPECT (monto > 0) ON VIOLATION DROP ROW
) AS
SELECT id, fecha, region, rut, monto FROM bronze.ventas;

-- Silver -> Gold: lo que consulta el rol "analistas" (con filtro y máscara).
CREATE MATERIALIZED VIEW gold.ventas AS
SELECT id, fecha, region, rut, monto FROM silver.ventas;

-- Gold aparte, con tope: si un total supera 1000 la actualización FALLA y
-- se conserva la versión anterior (el resto de Gold sigue refrescándose).
CREATE MATERIALIZED VIEW gold.ventas_por_region (
  CONSTRAINT total_bajo_tope EXPECT (total < 1000) ON VIOLATION FAIL UPDATE
) AS
SELECT region, sum(monto) AS total FROM silver.ventas GROUP BY region;
