ALTER TABLE gold.ventas ALTER COLUMN rut SET MASK '***' || right(rut, 2) FOR ROLE analistas
