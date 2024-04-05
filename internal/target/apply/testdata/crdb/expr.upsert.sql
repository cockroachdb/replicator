UPSERT INTO "database"."schema"."table"@{NO_FULL_SCAN} (
"pk0","pk1","val0","val1","enum","has_default"
) VALUES
($1::STRING,($2+$2)::INT8,('fixed')::STRING,($3||'foobar')::STRING,$4::"database"."schema"."MyEnum",CASE WHEN $5::BOOLEAN THEN $6::INT8 ELSE expr() END),
($7::STRING,($8+$8)::INT8,('fixed')::STRING,($9||'foobar')::STRING,$10::"database"."schema"."MyEnum",CASE WHEN $11::BOOLEAN THEN $12::INT8 ELSE expr() END)