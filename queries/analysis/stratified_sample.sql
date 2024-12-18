COPY (WITH buckets AS (
  SELECT 
    domain,
    freq,
    CASE 
      WHEN freq BETWEEN 1 AND 10 THEN '1-10'
      WHEN freq BETWEEN 11 AND 100 THEN '11-100'
      WHEN freq BETWEEN 101 AND 1000 THEN '101-1000'
      ELSE '1000+'
    END AS count_bucket
  FROM 'DATASET_PATTERN'
),
random_samples AS (
  SELECT 
    domain,
    freq,
    count_bucket,
    ROW_NUMBER() OVER (
      PARTITION BY count_bucket 
      ORDER BY RANDOM()
    ) as rn
  FROM buckets
)
SELECT 
  domain,
  freq,
  count_bucket
FROM random_samples
WHERE rn <= 100) TO 'OUTPUT_PATH';