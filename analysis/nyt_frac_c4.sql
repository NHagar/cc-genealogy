WITH nyt_tot AS (
    SELECT COUNT(*) AS tot FROM './c4/data/*.parquet' WHERE url LIKE '%nytimes.com%'
),
cc_tot AS (
    SELECT COUNT(*) AS tot FROM './c4/data/*.parquet'
)
SELECT nyt_tot.tot, cc_tot.tot, nyt_tot.tot / cc_tot.tot AS ratio FROM nyt_tot, cc_tot;
