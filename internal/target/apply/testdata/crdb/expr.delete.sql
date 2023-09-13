DELETE FROM "database"."schema"."table" WHERE ("pk0","pk1","ignored_pk")IN(($1::STRING,($2+$2)::INT8,$3::STRING),
($4::STRING,($5+$5)::INT8,$6::STRING))