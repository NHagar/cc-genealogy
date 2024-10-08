WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(url, '://', 2), '/', 1) AS domain,
FROM
    'hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet'
)
SELECT
    domain,
    COUNT(*) AS count
FROM
    domains
GROUP BY
    domain
ORDER BY
    count DESC;
