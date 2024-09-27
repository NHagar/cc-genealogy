SELECT
    url AS original_url,
    SPLIT_PART(url, '?', 1) AS url_without_params,
    SPLIT_PART(SPLIT_PART(url, '://', 2), '/', 1) AS domain,
FROM
    'hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet'
LIMIT 10;
