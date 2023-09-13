WITH data("pk0","pk1","val0","val1","geom","geog","enum","has_default") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum",CASE WHEN $8::BOOLEAN THEN $9::INT8 ELSE expr() END),
($10::STRING,$11::INT8,$12::STRING,$13::STRING,st_geomfromgeojson($14::JSONB),st_geogfromgeojson($15::JSONB),$16::"database"."schema"."MyEnum",CASE WHEN $17::BOOLEAN THEN $18::INT8 ELSE expr() END)),
deadlined AS (SELECT * FROM data WHERE("val0">now()-'1h0m0s'::INTERVAL)AND("val1">now()-'1s'::INTERVAL)),
current AS (
SELECT "pk0","pk1", "table"."val1","table"."val0"
FROM "database"."schema"."table"
JOIN deadlined
USING ("pk0","pk1")),
action AS (
SELECT deadlined.* FROM deadlined
LEFT JOIN current
USING ("pk0","pk1")
WHERE current."pk0" IS NULL OR
(deadlined."val1",deadlined."val0") > (current."val1",current."val0"))
INSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog","enum","has_default")
SELECT * FROM action
ON CONFLICT ( "pk0","pk1" )
DO UPDATE SET ("val0","val1","geom","geog","enum","has_default") = ROW(excluded."val0",excluded."val1",excluded."geom",excluded."geog",excluded."enum",excluded."has_default")