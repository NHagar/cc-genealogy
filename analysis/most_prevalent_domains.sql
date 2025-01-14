WITH 
    total 
AS (
    SELECT 
        SUM(url_count) AS tot 
    FROM 
        './cc_2024_18/*.parquet'
) 
SELECT 
    url_host_name AS domain, 
    url_count AS URLs, 
    url_count / (SELECT tot FROM total) * 100 AS 'Percent of URLs'
FROM 
    './cc_2024_18/*.parquet' 
ORDER BY 2 DESC
LIMIT 10;