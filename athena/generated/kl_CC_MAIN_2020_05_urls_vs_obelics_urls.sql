-- Step 1: Create p_materialized
DROP TABLE IF EXISTS p_materialized;

DROP TABLE IF EXISTS q_materialized;

CREATE TABLE p_materialized WITH (format = 'PARQUET') AS
WITH p_base AS (
    SELECT
        url_host_name AS domain,
        SUM(url_count) AS url_count
    FROM
        cc
    WHERE
        dataset_name IN ('CC-MAIN-2020-05_urls','CC-MAIN-2020-10_urls','CC-MAIN-2020-16_urls','CC-MAIN-2020-24_urls','CC-MAIN-2020-29_urls','CC-MAIN-2020-34_urls','CC-MAIN-2020-40_urls','CC-MAIN-2020-50_urls','CC-MAIN-2021-04_urls','CC-MAIN-2021-10_urls','CC-MAIN-2021-17_urls','CC-MAIN-2021-21_urls','CC-MAIN-2021-25_urls','CC-MAIN-2021-31_urls','CC-MAIN-2021-39_urls','CC-MAIN-2021-43_urls','CC-MAIN-2021-49_urls','CC-MAIN-2022-05_urls','CC-MAIN-2022-21_urls','CC-MAIN-2022-27_urls','CC-MAIN-2022-33_urls','CC-MAIN-2022-40_urls','CC-MAIN-2022-49_urls','CC-MAIN-2023-06_urls')
    GROUP BY
        url_host_name
),
p_total AS (
    SELECT SUM(url_count) AS total_url_count FROM p_base
)
SELECT
    pb.domain,
    pb.url_count,
    pb.url_count * 1.0 / pt.total_url_count AS p_prob
FROM
    p_base pb
CROSS JOIN
    p_total pt;

-- Step 2: Create q_materialized
CREATE TABLE q_materialized WITH (format = 'PARQUET') AS
WITH q_base AS (
    SELECT
        domain,
        COUNT(*) AS url_count
    FROM
        hf_data
    WHERE
        dataset_name IN ('obelics_urls')
    GROUP BY
        domain
),
q_total AS (
    SELECT SUM(url_count) AS total_url_count FROM q_base
)
SELECT
    qb.domain,
    qb.url_count,
    qb.url_count * 1.0 / qt.total_url_count AS q_prob
FROM
    q_base qb
CROSS JOIN
    q_total qt;

-- Step 3: Final Calculation
SELECT
    SUM(
        p.p_prob
        * LN(
            p.p_prob
            / COALESCE(q.q_prob, 1e-12)
        )
    ) AS kl_divergence
FROM
    p_materialized p
LEFT JOIN
    q_materialized q
USING (domain);