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
        dataset_name IN ('CC-MAIN-2013-20_urls','CC-MAIN-2013-48_urls','CC-MAIN-2014-10_urls','CC-MAIN-2014-15_urls','CC-MAIN-2014-23_urls','CC-MAIN-2014-35_urls','CC-MAIN-2014-41_urls','CC-MAIN-2014-42_urls','CC-MAIN-2014-49_urls','CC-MAIN-2014-52_urls','CC-MAIN-2015-06_urls','CC-MAIN-2015-11_urls','CC-MAIN-2015-14_urls','CC-MAIN-2015-18_urls','CC-MAIN-2015-22_urls','CC-MAIN-2015-27_urls','CC-MAIN-2015-32_urls','CC-MAIN-2015-35_urls','CC-MAIN-2015-40_urls','CC-MAIN-2015-48_urls','CC-MAIN-2016-07_urls','CC-MAIN-2016-18_urls','CC-MAIN-2016-22_urls','CC-MAIN-2016-26_urls','CC-MAIN-2016-30_urls','CC-MAIN-2016-36_urls','CC-MAIN-2016-40_urls','CC-MAIN-2016-44_urls','CC-MAIN-2016-50_urls','CC-MAIN-2017-04_urls','CC-MAIN-2017-09_urls','CC-MAIN-2017-13_urls','CC-MAIN-2017-17_urls','CC-MAIN-2017-22_urls','CC-MAIN-2017-26_urls','CC-MAIN-2017-30_urls','CC-MAIN-2017-34_urls','CC-MAIN-2017-39_urls','CC-MAIN-2017-43_urls','CC_MAIN_2017_47_urls','CC-MAIN-2017-51_urls','CC-MAIN-2018-05_urls','CC-MAIN-2018-09_urls','CC-MAIN-2018-13_urls','CC-MAIN-2018-17_urls','CC-MAIN-2018-22_urls','CC-MAIN-2018-26_urls','CC-MAIN-2018-30_urls','CC-MAIN-2018-34_urls','CC-MAIN-2018-39_urls','CC-MAIN-2018-43_urls','CC-MAIN-2018-47_urls','CC-MAIN-2018-51_urls','CC-MAIN-2019-04_urls','CC-MAIN-2019-09_urls','CC-MAIN-2019-13_urls','CC-MAIN-2019-18_urls','CC-MAIN-2019-22_urls','CC-MAIN-2019-26_urls','CC-MAIN-2019-30_urls','CC-MAIN-2019-35_urls','CC-MAIN-2019-39_urls','CC-MAIN-2019-43_urls','CC-MAIN-2019-47_urls','CC-MAIN-2019-51_urls','CC-MAIN-2020-05_urls','CC-MAIN-2020-10_urls','CC-MAIN-2020-16_urls','CC-MAIN-2020-24_urls','CC-MAIN-2020-29_urls','CC-MAIN-2020-34_urls','CC-MAIN-2020-40_urls','CC-MAIN-2020-50_urls','CC-MAIN-2021-04_urls','CC-MAIN-2021-10_urls','CC-MAIN-2021-17_urls','CC-MAIN-2021-21_urls','CC-MAIN-2021-25_urls','CC-MAIN-2021-31_urls','CC-MAIN-2021-39_urls','CC-MAIN-2021-43_urls','CC-MAIN-2021-49_urls','CC-MAIN-2022-05_urls','CC-MAIN-2022-21_urls','CC-MAIN-2022-27_urls','CC-MAIN-2022-33_urls','CC-MAIN-2022-40_urls','CC-MAIN-2022-49_urls','CC-MAIN-2023-06_urls','CC-MAIN-2023-14_urls')
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
        dataset_name IN ('open-web-math_urls')
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