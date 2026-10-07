WITH
"customers" AS (
  SELECT "id" AS "id", "region" AS "region", "name" AS "name"
  FROM "role_analistas"."customers" AS "customers"
),
"orders" AS (
  SELECT "id" AS "id", "status" AS "status", "amount" AS "amount", "created" AS "created", (strftime(created, '%Y-%m')) AS "month", "note" AS "secret_note", "customer_id"
  FROM "role_analistas"."orders" AS "orders"
),
"q0" AS (SELECT "customers"."region" AS "d0", SUM("orders"."amount") AS "m0", (SUM("orders"."amount")) / (COUNT(DISTINCT "orders"."id")) AS "m1"
  FROM "orders"
  LEFT JOIN "customers" ON "orders"."customer_id" = "customers"."id"
  WHERE "orders"."status" = 'paid'
  GROUP BY 1)
SELECT "q0"."d0" AS "region", "q0"."m0" AS "revenue", "q0"."m1" AS "avg_order"
FROM "q0"
ORDER BY "revenue" DESC
LIMIT 10
