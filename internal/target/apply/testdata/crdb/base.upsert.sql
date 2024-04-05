UPSERT INTO "database"."schema"."table"@{NO_FULL_SCAN} (
"pk0","pk1","val0","val1","geom","geog","enum","has_default"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum",CASE WHEN $8::BOOLEAN THEN $9::INT8 ELSE expr() END),
($10::STRING,$11::INT8,$12::STRING,$13::STRING,st_geomfromgeojson($14::JSONB),st_geogfromgeojson($15::JSONB),$16::"database"."schema"."MyEnum",CASE WHEN $17::BOOLEAN THEN $18::INT8 ELSE expr() END)