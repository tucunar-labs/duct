-- Depende de la ingesta ODBC (paso 2b).
CREATE MATERIALIZED VIEW silver.clientes AS
SELECT id, nombre, region FROM bronze.clientes;
