ALTER TABLE gold.ventas SET ROW FILTER region = 'CL' AND monto > 0 FOR ROLE analistas
