WITH raw_data("pk0","pk1","val0","val1","geom","geog","enum","has_default") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum",CASE WHEN $8::BOOLEAN THEN $9::INT8 ELSE expr() END),
($10::STRING,$11::INT8,$12::STRING,$13::STRING,st_geomfromgeojson($14::JSONB),st_geogfromgeojson($15::JSONB),$16::"database"."schema"."MyEnum",CASE WHEN $17::BOOLEAN THEN $18::INT8 ELSE expr() END)),
data AS (SELECT (row_number() OVER () - 1) __idx__, * FROM raw_data),
current AS (
SELECT "pk0","pk1", "table"@{NO_FULL_SCAN}."val1","table"@{NO_FULL_SCAN}."val0"
FROM "database"."schema"."table"@{NO_FULL_SCAN}
JOIN data
USING ("pk0","pk1")),
action AS (
SELECT data.* FROM data
LEFT JOIN current
USING ("pk0","pk1")
WHERE current."pk0" IS NULL OR
(data."val1",data."val0") > (current."val1",current."val0")),
upserted AS (
UPSERT INTO "database"."schema"."table"@{NO_FULL_SCAN} ("pk0","pk1","val0","val1","geom","geog","enum","has_default")
SELECT "pk0","pk1","val0","val1","geom","geog","enum","has_default" FROM action
RETURNING "pk0","pk1")
SELECT data.__idx__, t."pk0",t."pk1",t."val0",t."val1",t."geom",t."geog",t."enum",t."has_default" FROM "database"."schema"."table"@{NO_FULL_SCAN} t
JOIN data USING ("pk0","pk1")
LEFT JOIN upserted USING ("pk0","pk1")
WHERE upserted."pk0" IS NULL