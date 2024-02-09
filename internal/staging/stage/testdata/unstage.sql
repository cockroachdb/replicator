WITH hlc_0 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl0"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[1])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 1
),
hlc_1 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl1"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[2])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 1
),
hlc_2 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl2"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[3])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 1
),
hlc_3 (n, l) AS (
SELECT nanos, logical
FROM "_cdc_sink"."public"."my_db_public_tbl3"
WHERE (nanos, logical, key) > ($1, $2, ($5::STRING[])[4])
AND (nanos, logical) < ($3, $4)
AND NOT applied
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 1
),
hlc_all AS (SELECT n, l FROM hlc_0 UNION ALL
SELECT n, l FROM hlc_1 UNION ALL
SELECT n, l FROM hlc_2 UNION ALL
SELECT n, l FROM hlc_3),
hlc_min AS (SELECT n, l FROM hlc_all GROUP BY n, l ORDER BY n, l LIMIT 1),
blocked_0 AS (
SELECT key FROM "_cdc_sink"."public"."my_db_public_tbl0"
JOIN hlc_min ON (nanos,logical) = (n,l)
WHERE (lease IS NOT NULL AND lease > now())
GROUP BY key
),
blocked_1 AS (
SELECT key FROM "_cdc_sink"."public"."my_db_public_tbl1"
JOIN hlc_min ON (nanos,logical) = (n,l)
WHERE (lease IS NOT NULL AND lease > now())
GROUP BY key
),
blocked_2 AS (
SELECT key FROM "_cdc_sink"."public"."my_db_public_tbl2"
JOIN hlc_min ON (nanos,logical) = (n,l)
WHERE (lease IS NOT NULL AND lease > now())
GROUP BY key
),
blocked_3 AS (
SELECT key FROM "_cdc_sink"."public"."my_db_public_tbl3"
JOIN hlc_min ON (nanos,logical) = (n,l)
WHERE (lease IS NOT NULL AND lease > now())
GROUP BY key
),
data_0 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl0"
SET applied=true, lease=NULL
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[1])
AND NOT applied
AND key NOT IN (SELECT key FROM blocked_0)
RETURNING nanos, logical, key, mut, before),
data_1 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl1"
SET applied=true, lease=NULL
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[2])
AND NOT applied
AND key NOT IN (SELECT key FROM blocked_1)
RETURNING nanos, logical, key, mut, before),
data_2 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl2"
SET applied=true, lease=NULL
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[3])
AND NOT applied
AND key NOT IN (SELECT key FROM blocked_2)
RETURNING nanos, logical, key, mut, before),
data_3 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl3"
SET applied=true, lease=NULL
FROM hlc_min
WHERE (nanos,logical) = (n, l)
AND (nanos, logical, key) > ($1, $2, ($5::STRING[])[4])
AND NOT applied
AND key NOT IN (SELECT key FROM blocked_3)
RETURNING nanos, logical, key, mut, before)
SELECT * FROM (
SELECT 0 idx, nanos, logical, key, mut, before FROM data_0 UNION ALL
SELECT 1 idx, nanos, logical, key, mut, before FROM data_1 UNION ALL
SELECT 2 idx, nanos, logical, key, mut, before FROM data_2 UNION ALL
SELECT 3 idx, nanos, logical, key, mut, before FROM data_3)
ORDER BY nanos, logical, idx, key