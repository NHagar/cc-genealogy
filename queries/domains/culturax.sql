CREATE SECRET hf_token (TYPE HUGGINGFACE, TOKEN '{hf_token_value}');
CREATE TABLE domains AS (
WITH domains AS (
SELECT
    SPLIT_PART(SPLIT_PART(url, '://', 2), '/', 1) AS domain,
FROM
    'hf://datasets/uonlp/CulturaX/*/*.parquet'
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
    count DESC
)
