-- silver orders
CREATE MATERIALIZED VIEW silver.orders (
  CONSTRAINT id_present EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT positive EXPECT (amount > 0 AND amount < (SELECT max(x) FROM lim)) ON VIOLATION DROP ROW,
  CONSTRAINT "Odd Name" EXPECT (note <> ')' )
) AS
SELECT id, amount, note -- trailing; comment
FROM bronze.orders;
