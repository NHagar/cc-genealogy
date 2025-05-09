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
        dataset_name IN ({{ dataset_name }})
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
        dataset_name IN ({{ dataset_name }})
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