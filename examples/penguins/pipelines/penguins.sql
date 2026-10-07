-- Bronze -> Silver: tipos correctos, un id por pingüino y filas sin medidas fuera.
-- 2 de las 344 observaciones no tienen ninguna medida: se descartan (DROP ROW).
CREATE MATERIALIZED VIEW silver.penguins (
  CONSTRAINT especie_conocida EXPECT (species IN ('Adelie', 'Chinstrap', 'Gentoo')) ON VIOLATION DROP ROW,
  CONSTRAINT tiene_medidas EXPECT (bill_length_mm IS NOT NULL AND bill_depth_mm IS NOT NULL AND body_mass_g IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT masa_positiva EXPECT (coalesce(body_mass_g, 1) > 0) ON VIOLATION DROP ROW
) AS
SELECT
  row_number() OVER (ORDER BY year, species, island, bill_length_mm, bill_depth_mm, body_mass_g) AS id,
  species::VARCHAR AS species,
  island::VARCHAR AS island,
  coalesce(sex, 'unknown')::VARCHAR AS sex,
  year::INTEGER AS year,
  bill_length_mm::DOUBLE AS bill_length_mm,
  bill_depth_mm::DOUBLE AS bill_depth_mm,
  flipper_length_mm::INTEGER AS flipper_length_mm,
  body_mass_g::INTEGER AS body_mass_g
FROM bronze.penguins;

-- Silver -> Gold: detalle por pingüino (lo que consume la capa semántica).
CREATE MATERIALIZED VIEW gold.penguins AS
SELECT id, species, island, sex, year, bill_length_mm, bill_depth_mm,
       flipper_length_mm, body_mass_g
FROM silver.penguins;

-- Gold: medidas por especie, isla y sexo (tabla con contrato).
CREATE MATERIALIZED VIEW gold.penguin_stats AS
SELECT species, island, sex,
       count(*)::BIGINT AS n,
       round(avg(bill_length_mm), 2) AS avg_bill_length_mm,
       round(avg(bill_depth_mm), 2) AS avg_bill_depth_mm,
       round(avg(flipper_length_mm), 1) AS avg_flipper_length_mm,
       round(avg(body_mass_g), 0) AS avg_body_mass_g
FROM silver.penguins
GROUP BY species, island, sex;
