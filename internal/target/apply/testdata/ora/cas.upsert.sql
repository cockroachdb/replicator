MERGE INTO "schema"."table" USING (
WITH data ("pk0","pk1","val0","val1","has_default") AS (
SELECT CAST(:p1 AS VARCHAR(256)), CAST(:p2 AS INT), CAST(:p3 AS VARCHAR(256)), CAST(:p4 AS VARCHAR(256)), CASE WHEN :p5 IS NOT NULL THEN CAST(:p6 AS INT8) ELSE expr() END FROM DUAL UNION ALL 
SELECT CAST(:p7 AS VARCHAR(256)), CAST(:p8 AS INT), CAST(:p9 AS VARCHAR(256)), CAST(:p10 AS VARCHAR(256)), CASE WHEN :p11 IS NOT NULL THEN CAST(:p12 AS INT8) ELSE expr() END FROM DUAL
),
active AS (
SELECT "pk0","pk1", "table"."val1","table"."val0"
FROM "schema"."table" JOIN data USING ("pk0","pk1")),
action AS (
SELECT "pk0","pk1", data."val0",data."val1",data."has_default" FROM data
LEFT JOIN active USING ("pk0","pk1") WHERE active."val1" IS NULL OR
(data."val1",data."val0") > (active."val1",active."val0"))
SELECT * FROM action) x ON ("schema"."table"."pk0" = x."pk0" AND "schema"."table"."pk1" = x."pk1")
WHEN NOT MATCHED THEN INSERT ("pk0","pk1","val0","val1","has_default") VALUES (x."pk0", x."pk1", x."val0", x."val1", x."has_default")
WHEN MATCHED THEN UPDATE SET "val0" = x."val0", "val1" = x."val1", "has_default" = x."has_default"