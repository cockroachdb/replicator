MERGE INTO "schema"."table" USING (
WITH data ("pk0","pk1","val0","val1","has_default") AS (
SELECT CAST(:p1 AS VARCHAR(256)), CAST(:p2 AS INT), CAST(:p3 AS VARCHAR(256)), CAST(:p4 AS VARCHAR(256)), CASE WHEN :p5 IS NOT NULL THEN CAST(:p6 AS INT8) ELSE expr() END FROM DUAL
)
SELECT * FROM data) x ON ("schema"."table"."pk0" = x."pk0" AND "schema"."table"."pk1" = x."pk1")
WHEN NOT MATCHED THEN INSERT ("pk0","pk1","val0","val1","has_default") VALUES (x."pk0", x."pk1", x."val0", x."val1", x."has_default")
WHEN MATCHED THEN UPDATE SET "val0" = x."val0", "val1" = x."val1", "has_default" = x."has_default"