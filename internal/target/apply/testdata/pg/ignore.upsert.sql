INSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","enum","has_default"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,$5::"database"."schema"."MyEnum",CASE WHEN $6::INT = 1 THEN $7::INT8 ELSE expr() END),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,$12::"database"."schema"."MyEnum",CASE WHEN $13::INT = 1 THEN $14::INT8 ELSE expr() END)
ON CONFLICT ( "pk0","pk1" )
DO UPDATE SET ("val0","val1","enum","has_default") = ROW(excluded."val0",excluded."val1",excluded."enum",excluded."has_default")