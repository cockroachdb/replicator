WITH candidates_0 AS (
SELECT nanos, logical, key, lease
FROM "_cdc_sink"."public"."my_db_public_tbl0"
WHERE (nanos, logical, key) > (($1::INT8[])[1], ($2::INT8[])[1], ($5::STRING[])[1])
AND (nanos, logical) < ($3, $4)
AND NOT applied
FOR UPDATE
),
candidates_1 AS (
SELECT nanos, logical, key, lease
FROM "_cdc_sink"."public"."my_db_public_tbl1"
WHERE (nanos, logical, key) > (($1::INT8[])[2], ($2::INT8[])[2], ($5::STRING[])[2])
AND (nanos, logical) < ($3, $4)
AND NOT applied
FOR UPDATE
),
candidates_2 AS (
SELECT nanos, logical, key, lease
FROM "_cdc_sink"."public"."my_db_public_tbl2"
WHERE (nanos, logical, key) > (($1::INT8[])[3], ($2::INT8[])[3], ($5::STRING[])[3])
AND (nanos, logical) < ($3, $4)
AND NOT applied
FOR UPDATE
),
candidates_3 AS (
SELECT nanos, logical, key, lease
FROM "_cdc_sink"."public"."my_db_public_tbl3"
WHERE (nanos, logical, key) > (($1::INT8[])[4], ($2::INT8[])[4], ($5::STRING[])[4])
AND (nanos, logical) < ($3, $4)
AND NOT applied
FOR UPDATE
),
hlc_all AS (
SELECT nanos, logical FROM (SELECT nanos, logical FROM candidates_0 UNION ALL
SELECT nanos, logical FROM candidates_1 UNION ALL
SELECT nanos, logical FROM candidates_2 UNION ALL
SELECT nanos, logical FROM candidates_3)
GROUP BY nanos, logical
ORDER BY nanos, logical
LIMIT 1
),
target_0 AS (
SELECT * FROM candidates_0 c
JOIN hlc_all USING (nanos, logical)),
target_1 AS (
SELECT * FROM candidates_1 c
JOIN hlc_all USING (nanos, logical)),
target_2 AS (
SELECT * FROM candidates_2 c
JOIN hlc_all USING (nanos, logical)),
target_3 AS (
SELECT * FROM candidates_3 c
JOIN hlc_all USING (nanos, logical)),
data_0 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl0" s
SET applied=true, lease=NULL
FROM target_0 t
WHERE (s.nanos, s.logical, s.key) = (t.nanos, t.logical, t.key)
RETURNING s.nanos, s.logical, s.key, s.mut, s.before
),
data_1 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl1" s
SET applied=true, lease=NULL
FROM target_1 t
WHERE (s.nanos, s.logical, s.key) = (t.nanos, t.logical, t.key)
RETURNING s.nanos, s.logical, s.key, s.mut, s.before
),
data_2 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl2" s
SET applied=true, lease=NULL
FROM target_2 t
WHERE (s.nanos, s.logical, s.key) = (t.nanos, t.logical, t.key)
RETURNING s.nanos, s.logical, s.key, s.mut, s.before
),
data_3 AS (
UPDATE "_cdc_sink"."public"."my_db_public_tbl3" s
SET applied=true, lease=NULL
FROM target_3 t
WHERE (s.nanos, s.logical, s.key) = (t.nanos, t.logical, t.key)
RETURNING s.nanos, s.logical, s.key, s.mut, s.before
)
SELECT * FROM (
SELECT 0 idx, nanos, logical, key, mut, before FROM data_0 UNION ALL
SELECT 1 idx, nanos, logical, key, mut, before FROM data_1 UNION ALL
SELECT 2 idx, nanos, logical, key, mut, before FROM data_2 UNION ALL
SELECT 3 idx, nanos, logical, key, mut, before FROM data_3)
ORDER BY nanos, logical, idx, key