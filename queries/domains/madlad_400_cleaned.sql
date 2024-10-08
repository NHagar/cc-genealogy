WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(url, '://', 2), '/', 1) AS domain,
FROM
-- TODO: I think this is the cleaned split, need to verify
    READ_JSON('hf://datasets/allenai/MADLAD-400/data-v1p5/*/*.jsonl.gz')
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
