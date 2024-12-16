COPY (WITH buckets AS (
  SELECT 
    domain,
    count,
    CASE 
      WHEN count BETWEEN 1 AND 10 THEN '1-10'
      WHEN count BETWEEN 11 AND 100 THEN '11-100'
      WHEN count BETWEEN 101 AND 1000 THEN '101-1000'
      ELSE '1000+'
    END AS count_bucket
  FROM 'c4_en_urls_domains.csv'
),
random_samples AS (
  SELECT 
    domain,
    count,
    count_bucket,
    ROW_NUMBER() OVER (
      PARTITION BY count_bucket 
      ORDER BY RANDOM()
    ) as rn
  FROM buckets
)
SELECT 
  domain,
  count,
  count_bucket
FROM random_samples
WHERE rn <= 100) TO 'c4_en_urls_sample.csv';