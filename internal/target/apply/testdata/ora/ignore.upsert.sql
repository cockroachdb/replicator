MERGE INTO "schema"."table" USING (
WITH data ("pk0","pk1","has_default") AS (
SELECT CAST(:1 AS VARCHAR(256)), CAST(:2 AS INT), CASE WHEN :3 = 1 THEN CAST(:4 AS INT8) ELSE expr() END FROM DUAL
)
SELECT * FROM data) x ON ("schema"."table"."pk0" = x."pk0" AND "schema"."table"."pk1" = x."pk1")
WHEN NOT MATCHED THEN INSERT ("pk0","pk1","has_default") VALUES (x."pk0", x."pk1", x."has_default")
WHEN MATCHED THEN UPDATE SET "has_default" = x."has_default"