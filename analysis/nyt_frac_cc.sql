WITH nyt_tot AS (
    SELECT SUM(url_count) AS tot FROM './cc_2019_18/*.parquet' WHERE url_host_name LIKE '%nytimes.com%'
),
cc_tot AS (
    SELECT SUM(url_count) AS tot FROM './cc_2019_18/*.parquet'
)
SELECT nyt_tot.tot, cc_tot.tot, nyt_tot.tot / cc_tot.tot AS ratio FROM nyt_tot, cc_tot;
