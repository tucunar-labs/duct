WITH
"customers" AS (
  SELECT "id" AS "id", "region" AS "region", "name" AS "name"
  FROM "role_analistas"."customers" AS "customers"
),
"orders" AS (
  SELECT "id" AS "id", "status" AS "status", "amount" AS "amount", "created" AS "created", (strftime(created, '%Y-%m')) AS "month", "note" AS "secret_note", "customer_id"
  FROM "role_analistas"."orders" AS "orders"
),
"order_items" AS (
  SELECT "id" AS "id", "sku" AS "sku", "qty" AS "qty", (qty * price) AS "line_total", "order_id"
  FROM "role_analistas"."order_items" AS "order_items"
),
"q0" AS (SELECT "customers"."region" AS "d0", SUM("orders"."amount") AS "m0"
  FROM "orders"
  LEFT JOIN "customers" ON "orders"."customer_id" = "customers"."id"
  GROUP BY 1),
"q1" AS (SELECT "customers"."region" AS "d0", SUM("order_items"."qty") AS "m1"
  FROM "order_items"
  LEFT JOIN "orders" ON "order_items"."order_id" = "orders"."id"
  LEFT JOIN "customers" ON "orders"."customer_id" = "customers"."id"
  GROUP BY 1)
SELECT COALESCE("q0"."d0", "q1"."d0") AS "region", ("q0"."m0") / ("q1"."m1") AS "revenue_per_unit"
FROM "q0"
FULL OUTER JOIN "q1" ON "q0"."d0" IS NOT DISTINCT FROM "q1"."d0"
ORDER BY "revenue_per_unit" DESC
LIMIT 500
