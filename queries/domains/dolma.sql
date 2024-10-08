WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(metadata.url, '://', 2), '/', 1) AS domain,
FROM
    READ_JSON('{url}')
)
SELECT
    domain,
    COUNT(*) AS count
FROM
    domains
GROUP BY
    domain
ORDER BY
    count DESC
