WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(url, '://', 2), '/', 1) AS domain,
FROM
-- TODO: Will need a coordinator/params to run for every file
    READ_JSON('https://data.together.xyz/redpajama-data-v2/v1.0.0/documents/2014-15/0000/en_head.json.gz')
LIMIT 1000
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
