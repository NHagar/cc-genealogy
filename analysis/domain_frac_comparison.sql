WITH 
    total 
AS (
    SELECT 
        SUM(url_count) AS tot 
    FROM 
        './cc_2024_18/*.parquet'
),
percents AS (
    SELECT 
        url_host_name, 
        url_count, 
        url_count / (SELECT tot FROM total) AS pct 
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
),
med AS (
SELECT MEDIAN(pct) AS pct, MEDIAN(url_count) AS cnt FROM percents
),
top_tot AS (
SELECT SUM(pct) AS pct FROM most_prevalent
)
SELECT med.pct AS med_pct, med.cnt AS med_cnt, top_tot.pct / med.pct AS ratio 
FROM top_tot, med;