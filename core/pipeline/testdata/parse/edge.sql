/* header ; with semicolon /* nested */ still comment */
create or replace materialized view if not exists "My Schema"."Tab""le" as
  WITH a AS (SELECT ';' AS s, $$x;y$$ AS d) SELECT * FROM a;;

CREATE STREAMING TABLE events AS SELECT 1 AS one
