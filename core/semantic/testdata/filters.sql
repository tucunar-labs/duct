WITH
"customers" AS (
  SELECT "id" AS "id", "region" AS "region", "name" AS "name"
  FROM "role_analistas"."customers" AS "customers"
),
"orders" AS (
  SELECT "id" AS "id", "status" AS "status", "amount" AS "amount", "created" AS "created", (strftime(created, '%Y-%m')) AS "month", "note" AS "secret_note", "customer_id"
  FROM "role_analistas"."orders" AS "orders"
),
"q0" AS (SELECT SUM("orders"."amount") AS "m0"
  FROM "orders"
  LEFT JOIN "customers" ON "orders"."customer_id" = "customers"."id"
  WHERE "orders"."created" BETWEEN '2026-01-01' AND '2026-03-31' AND "customers"."name" LIKE 'O''Brien%' AND "orders"."amount" >= 100 AND "customers"."region" IS NOT NULL AND "orders"."status" NOT IN ('void', '''; DROP TABLE x; --'))
SELECT "q0"."m0" AS "revenue"
FROM "q0"
LIMIT 500
