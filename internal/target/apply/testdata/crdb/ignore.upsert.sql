WITH data ("pk0","pk1","val0","val1","enum","has_default") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,$5::"database"."schema"."MyEnum",CASE WHEN $6::BOOLEAN THEN $7::INT8 ELSE expr() END),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,$12::"database"."schema"."MyEnum",CASE WHEN $13::BOOLEAN THEN $14::INT8 ELSE expr() END)),
action AS (
SELECT data."pk0",data."pk1",CASE WHEN data."val0"='"__cdc__toast__"'::STRING
            THEN current."val0"  ELSE data."val0"
            END,CASE WHEN data."val1"='"__cdc__toast__"'::STRING
            THEN current."val1"  ELSE data."val1"
            END,data."enum",data."has_default"
FROM data
LEFT JOIN  "database"."schema"."table" as current
USING ("pk0","pk1"))  

UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","enum","has_default"
) 
SELECT "pk0","pk1","val0","val1","enum","has_default"  FROM action 