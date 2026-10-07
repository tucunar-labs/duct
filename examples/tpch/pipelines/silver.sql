-- Bronze -> Silver: TPC-H ya viene limpio; las EXPECT documentan lo que se asume.
CREATE MATERIALIZED VIEW silver.region AS
SELECT r_regionkey AS region_key, r_name AS region FROM bronze.region;

CREATE MATERIALIZED VIEW silver.nation (
  CONSTRAINT region_existe EXPECT (region_key IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT n_nationkey AS nation_key, n_name AS nation, n_regionkey AS region_key FROM bronze.nation;

CREATE MATERIALIZED VIEW silver.customer (
  CONSTRAINT segmento_conocido EXPECT (segment IN ('AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY')) ON VIOLATION DROP ROW,
  CONSTRAINT con_nacion EXPECT (nation_key IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT c_custkey AS customer_key, c_name AS customer, c_nationkey AS nation_key,
       c_mktsegment AS segment, c_acctbal AS acct_balance
FROM bronze.customer;

CREATE MATERIALIZED VIEW silver.orders (
  CONSTRAINT total_positivo EXPECT (total_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT estado_valido EXPECT (status IN ('F', 'O', 'P')) ON VIOLATION DROP ROW
) AS
SELECT o_orderkey AS order_key, o_custkey AS customer_key, o_orderstatus AS status,
       o_totalprice AS total_price, o_orderdate AS order_date
FROM bronze.orders;

CREATE MATERIALIZED VIEW silver.lineitem (
  CONSTRAINT cantidad_positiva EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT descuento_valido EXPECT (discount BETWEEN 0 AND 0.10) ON VIOLATION DROP ROW,
  CONSTRAINT precio_positivo EXPECT (extended_price > 0) ON VIOLATION DROP ROW
) AS
SELECT l_orderkey AS order_key, l_linenumber AS line_number, l_quantity AS quantity,
       l_extendedprice AS extended_price, l_discount AS discount,
       l_extendedprice * (1 - l_discount) AS net_revenue
FROM bronze.lineitem;
