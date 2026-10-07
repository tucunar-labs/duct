-- Silver -> Gold: viajes e ingresos por día, hora, borough de salida y forma de pago.
-- Se guardan sumas (no promedios) para poder promediar después en la capa semántica.
-- Contrato: contracts/trips_by_hour.odcs.yaml
CREATE MATERIALIZED VIEW gold.trips_by_hour AS
SELECT
  t.pickup_at::DATE AS pickup_date,
  hour(t.pickup_at)::INTEGER AS pickup_hour,
  coalesce(z.borough, 'Unknown') AS borough,
  t.payment_type,
  count(*)::BIGINT AS trips,
  round(sum(t.total_amount), 2) AS revenue,
  round(sum(t.fare_amount), 2) AS fare_sum,
  round(sum(t.trip_distance), 2) AS distance_sum,
  round(sum(t.tip_amount), 2) AS tip_sum
FROM silver.trips t
LEFT JOIN silver.zones z ON z.location_id = t.pickup_zone_id
GROUP BY ALL;

-- Zonas de salida con más viajes.
CREATE MATERIALIZED VIEW gold.top_zones AS
SELECT
  coalesce(z.zone, 'Unknown') AS zone,
  coalesce(z.borough, 'Unknown') AS borough,
  count(*)::BIGINT AS trips,
  round(sum(t.fare_amount), 2) AS fare_sum
FROM silver.trips t
LEFT JOIN silver.zones z ON z.location_id = t.pickup_zone_id
GROUP BY 1, 2
ORDER BY trips DESC
LIMIT 50;
