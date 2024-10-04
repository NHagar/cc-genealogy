WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(metadata.url, '://', 2), '/', 1) AS domain,
FROM
-- TODO: Will need a coordinator/params to run for every file
    READ_JSON('https://olmo-data.org/dolma-v1_7/c4-filtered/c4-0000.json.gz')
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
