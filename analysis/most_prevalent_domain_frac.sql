WITH 
    total 
AS (
    SELECT 
        SUM(url_count) AS tot 
    FROM 
        './cc_2024_18/*.parquet'
),
most_prevalent AS (
    SELECT 
        url_host_name, 
        url_count, 
        url_count / (SELECT tot FROM total) AS pct 
    FROM 
        './cc_2024_18/*.parquet' 
    ORDER BY 2 DESC
    LIMIT 10
)
SELECT SUM(pct), MEDIAN(url_count) FROM most_prevalent;