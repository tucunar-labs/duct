WITH "r" AS (SELECT "region" AS "region", SUM("revenue") AS "revenue", SUM("revenue") AS "__h0"
FROM "semantic_cache"."ventas__ventas_region"
WHERE "status" = 'paid'
GROUP BY 1)
SELECT "region", "revenue"
FROM "r"
WHERE "__h0" > 10
ORDER BY "region"
LIMIT 500
