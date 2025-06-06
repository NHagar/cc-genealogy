WITH p_base AS (
    SELECT
        domain,
        COUNT(*) AS url_count
    FROM
        hf_data
    WHERE
        dataset_name IN ('fineweb_urls')
    GROUP BY
        domain
),
q_base AS (
    SELECT
        domain,
        COUNT(*) AS url_count
    FROM
        hf_data
    WHERE
        dataset_name IN ('onlysports_dataset_urls')
    GROUP BY
        domain
),
p AS (
    SELECT
        domain,
        url_count,
        url_count * 1.0 / SUM(url_count) OVER () AS p_prob
    FROM
        p_base
),
q AS (
    SELECT
        domain,
        url_count,
        url_count * 1.0 / SUM(url_count) OVER () AS q_prob
    FROM
        q_base
)
SELECT
SUM(
    p_prob
    * LN(
        p_prob
        / COALESCE(q_prob, 1e-12)       -- smoothing to avoid div/0
    )
) AS kl_divergence
FROM
    p
LEFT JOIN
    q
USING (domain)