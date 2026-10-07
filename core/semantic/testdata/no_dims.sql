WITH
"orders" AS (
  SELECT "id" AS "id", "status" AS "status", "amount" AS "amount", "created" AS "created", (strftime(created, '%Y-%m')) AS "month", "note" AS "secret_note", "customer_id"
  FROM "role_analistas"."orders" AS "orders"
),
"q0" AS (SELECT COUNT(DISTINCT "orders"."id") AS "m0"
  FROM "orders")
SELECT "q0"."m0" AS "order_count"
FROM "q0"
LIMIT 500
