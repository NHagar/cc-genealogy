WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(url, '://', 2), '/', 1) AS domain,
FROM
    '{fpath}'
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
