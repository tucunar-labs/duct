ALTER TABLE gold.ventas SET ROW FILTER substring(region FROM 1 FOR 2) = 'CL' -- FOR ROLE otro
 FOR ROLE analistas;
