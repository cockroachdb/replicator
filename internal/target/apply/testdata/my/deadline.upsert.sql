INSERT
INTO "schema"."table"("pk0","pk1","val0","val1","has_default")
WITH data  ("pk0","pk1","val0","val1","has_default") AS (
  SELECT ?,?,?,?,CASE WHEN ? = 1 THEN ? ELSE expr() END
  UNION SELECT ?,?,?,?,CASE WHEN ? = 1 THEN ? ELSE expr() END
),
deadlined AS (SELECT * FROM data WHERE("val0"> now()- INTERVAL '3600' SECOND)AND("val1"> now()- INTERVAL '1' SECOND))
SELECT * FROM deadlined
ON DUPLICATE KEY UPDATE
"val0"=VALUES("val0"),"val1"=VALUES("val1"),"has_default"=VALUES("has_default")