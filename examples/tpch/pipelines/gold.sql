-- Silver -> Gold: ingresos netos (precio x (1 - descuento)) por mes, segmento, región y nación.
-- El contrato contracts/revenue_by_nation.odcs.yaml se hace cumplir en cada refresco.
CREATE MATERIALIZED VIEW gold.revenue_by_nation AS
SELECT
  date_trunc('month', o.order_date)::DATE AS order_month,
  c.segment,
  r.region,
  n.nation,
  count(DISTINCT o.order_key)::BIGINT AS orders,
  count(*)::BIGINT AS lines,
  round(sum(l.net_revenue), 2) AS revenue
FROM silver.lineitem l
JOIN silver.orders o ON o.order_key = l.order_key
JOIN silver.customer c ON c.customer_key = o.customer_key
JOIN silver.nation n ON n.nation_key = c.nation_key
JOIN silver.region r ON r.region_key = n.region_key
GROUP BY 1, 2, 3, 4;

-- Los 100 clientes que más compraron en toda la historia.
CREATE MATERIALIZED VIEW gold.top_customers AS
SELECT
  rank() OVER (ORDER BY sum(l.net_revenue) DESC)::INTEGER AS rank,
  c.customer,
  c.segment,
  n.nation,
  r.region,
  count(DISTINCT o.order_key)::BIGINT AS orders,
  round(sum(l.net_revenue), 2) AS revenue
FROM silver.lineitem l
JOIN silver.orders o ON o.order_key = l.order_key
JOIN silver.customer c ON c.customer_key = o.customer_key
JOIN silver.nation n ON n.nation_key = c.nation_key
JOIN silver.region r ON r.region_key = n.region_key
GROUP BY c.customer, c.segment, n.nation, r.region
QUALIFY rank <= 100;
