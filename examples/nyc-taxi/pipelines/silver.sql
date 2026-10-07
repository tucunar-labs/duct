-- Bronze -> Silver. El archivo de enero 2024 trae basura conocida: viajes con fecha
-- de 2002 o 2008, tarifas negativas (reembolsos) y llegadas antes de la salida.
CREATE MATERIALIZED VIEW silver.zones AS
SELECT LocationID::INTEGER AS location_id,
       coalesce(nullif(Borough, 'N/A'), 'Unknown') AS borough,
       Zone AS zone,
       service_zone
FROM bronze.zones;

CREATE MATERIALIZED VIEW silver.trips (
  CONSTRAINT tarifa_no_negativa EXPECT (fare_amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT total_no_negativo EXPECT (total_amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT propina_no_negativa EXPECT (tip_amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT distancia_razonable EXPECT (trip_distance >= 0 AND trip_distance < 500) ON VIOLATION DROP ROW,
  CONSTRAINT llega_despues_de_salir EXPECT (dropoff_at >= pickup_at) ON VIOLATION DROP ROW,
  CONSTRAINT fecha_en_2024 EXPECT (year(pickup_at) = 2024) ON VIOLATION DROP ROW,
  CONSTRAINT zona_de_salida EXPECT (pickup_zone_id IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT
  tpep_pickup_datetime AS pickup_at,
  tpep_dropoff_datetime AS dropoff_at,
  PULocationID AS pickup_zone_id,
  DOLocationID AS dropoff_zone_id,
  passenger_count::INTEGER AS passengers,
  trip_distance,
  fare_amount,
  tip_amount,
  total_amount,
  CASE payment_type
    WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash' WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute' WHEN 6 THEN 'Voided' ELSE 'Unknown' END AS payment_type
FROM bronze.trips;
