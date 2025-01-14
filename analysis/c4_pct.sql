WITH c4_total AS (
    SELECT
        COUNT(*) AS c4_tot
    FROM
        './c4/data/*.parquet'
),
cc_total AS (
    SELECT
        SUM(url_count) AS cc_tot
    FROM
        './cc_2019_18/*.parquet'
)
SELECT
    c4_tot,
    cc_tot,
    c4_tot / cc_tot AS ratio
FROM
    c4_total, cc_total;