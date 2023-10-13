INSERT
INTO "schema"."table"("pk0","pk1","val0","val1","has_default")
WITH data AS (
  SELECT ? AS "pk0",? AS "pk1",? AS "val0",? AS "val1",CASE WHEN ? THEN ? ELSE expr() END AS "has_default"
  UNION SELECT ? AS "pk0",? AS "pk1",? AS "val0",? AS "val1",CASE WHEN ? THEN ? ELSE expr() END AS "has_default"
),
current AS (
SELECT "pk0","pk1", "table"."val1","table"."val0"
FROM "schema"."table"
JOIN data
USING ("pk0","pk1")),
action AS (
SELECT data.* FROM data
LEFT JOIN current
USING ("pk0","pk1")
WHERE current."pk0" IS NULL OR
(data."val1",data."val0") > (current."val1",current."val0"))
SELECT * FROM action

ON DUPLICATE KEY UPDATE 
"val0"=VALUES("val0"),"val1"=VALUES("val1"),"has_default"=VALUES("has_default")