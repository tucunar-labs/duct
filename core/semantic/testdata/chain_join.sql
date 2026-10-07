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
"q0" AS (SELECT "customers"."name" AS "d0", SUM("order_items"."line_total") AS "m0"
  FROM "order_items"
  LEFT JOIN "orders" ON "order_items"."order_id" = "orders"."id"
  LEFT JOIN "customers" ON "orders"."customer_id" = "customers"."id"
  WHERE "customers"."region" IN ('CL', 'AR')
  GROUP BY 1)
SELECT "q0"."d0" AS "name", "q0"."m0" AS "item_revenue"
FROM "q0"
ORDER BY "name"
LIMIT 500
