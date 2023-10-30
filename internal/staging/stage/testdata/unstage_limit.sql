WITH hlc_0 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl0"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[1])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 22
),
hlc_1 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl1"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[2])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 22
),
hlc_2 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl2"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[3])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 22
),
hlc_3 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl3"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[4])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 22
),
hlc_all AS (SELECT n, l FROM hlc_0 UNION ALL
SELECT n, l FROM hlc_1 UNION ALL
SELECT n, l FROM hlc_2 UNION ALL
SELECT n, l FROM hlc_3),
hlc_min AS (SELECT n, l FROM hlc_all GROUP BY n, l ORDER BY n, l LIMIT 22),
data_0 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl0"
SET applied=true
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[1])
AND NOT applied
ORDER BY nanos, logical, key
LIMIT 10000
RETURNING nanos, logical, key, mut, before),
data_1 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl1"
SET applied=true
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[2])
AND NOT applied
ORDER BY nanos, logical, key
LIMIT 10000
RETURNING nanos, logical, key, mut, before),
data_2 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl2"
SET applied=true
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[3])
AND NOT applied
ORDER BY nanos, logical, key
LIMIT 10000
RETURNING nanos, logical, key, mut, before),
data_3 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl3"
SET applied=true
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[4])
AND NOT applied
ORDER BY nanos, logical, key
LIMIT 10000
RETURNING nanos, logical, key, mut, before)
SELECT * FROM (
SELECT 0 idx, nanos, logical, key, mut, before FROM data_0 UNION ALL
SELECT 1 idx, nanos, logical, key, mut, before FROM data_1 UNION ALL
SELECT 2 idx, nanos, logical, key, mut, before FROM data_2 UNION ALL
SELECT 3 idx, nanos, logical, key, mut, before FROM data_3)
ORDER BY nanos, logical, idx, key