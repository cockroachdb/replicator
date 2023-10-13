INSERT INTO "schema"."table"
("pk0","pk1","val0","val1","has_default")
VALUES
(?,?,?,?,CASE WHEN ? THEN ? ELSE expr() END),
(?,?,?,?,CASE WHEN ? THEN ? ELSE expr() END)
ON DUPLICATE KEY UPDATE 
"val0"=VALUES("val0"),"val1"=VALUES("val1"),"has_default"=VALUES("has_default")