-- Tableau Export 03
-- Top commodities by shipment value

SET @YEAR = 2024;

WITH commodity_totals AS (
    SELECT
        f.year,
        f.commodity_id,
        SUM(f.value) AS total_value,
        SUM(f.current_value) AS total_current_value,
        SUM(f.tons) AS total_tons,
        SUM(f.tmiles) AS total_tmiles
    FROM fact_faf AS f
    WHERE f.year = @YEAR
    GROUP BY
        f.year,
        f.commodity_id
),
grand_total AS (
    SELECT
        SUM(total_value) AS grand_total_value
    FROM commodity_totals
)
SELECT
    ct.year,
    c.sctg2,
    c.commodity_name,
    ct.total_value,
    ct.total_current_value,
    ct.total_tons,
    ct.total_tmiles,
    ROUND((ct.total_value / NULLIF(gt.grand_total_value, 0)) * 100, 2) AS value_share_pct
FROM commodity_totals AS ct
JOIN dim_commodity AS c
    ON ct.commodity_id = c.commodity_id
CROSS JOIN grand_total AS gt
ORDER BY ct.total_value DESC
LIMIT 20;