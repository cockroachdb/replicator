WITH data ("pk0","pk1","val0","val1","enum","has_default") AS (
VALUES
($1::STRING,($2+$2)::INT8,('fixed')::STRING,($3||'foobar')::STRING,$4::"database"."schema"."MyEnum",CASE WHEN $5::BOOLEAN THEN $6::INT8 ELSE expr() END),
($7::STRING,($8+$8)::INT8,('fixed')::STRING,($9||'foobar')::STRING,$10::"database"."schema"."MyEnum",CASE WHEN $11::BOOLEAN THEN $12::INT8 ELSE expr() END)),
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