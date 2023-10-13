INSERT INTO "schema"."table"
("pk0","pk1","has_default")
VALUES
(?,?,CASE WHEN ? THEN ? ELSE expr() END),
(?,?,CASE WHEN ? THEN ? ELSE expr() END)
ON DUPLICATE KEY UPDATE 
"has_default"=VALUES("has_default")