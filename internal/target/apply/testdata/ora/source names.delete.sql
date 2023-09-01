DELETE FROM "schema"."table" WHERE ("pk0","pk1","ignored_pk")IN((CAST(:p1 AS VARCHAR(256)),CAST(:p2 AS INT),CAST(:p3 AS INT)),
(CAST(:p4 AS VARCHAR(256)),CAST(:p5 AS INT),CAST(:p6 AS INT)))