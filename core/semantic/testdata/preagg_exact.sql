SELECT "region" AS "region", "status" AS "status", "revenue" AS "revenue", "avg_order" AS "avg_order"
FROM "semantic_cache"."ventas__ventas_region"
ORDER BY "region", "status"
LIMIT 500
