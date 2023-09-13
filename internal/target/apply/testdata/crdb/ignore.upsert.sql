UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","enum","has_default"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,$5::"database"."schema"."MyEnum",CASE WHEN $6::BOOLEAN THEN $7::INT8 ELSE expr() END),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,$12::"database"."schema"."MyEnum",CASE WHEN $13::BOOLEAN THEN $14::INT8 ELSE expr() END)