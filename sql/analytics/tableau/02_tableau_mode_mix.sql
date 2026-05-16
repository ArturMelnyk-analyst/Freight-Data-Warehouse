-- Tableau Export 02
-- Transportation mode mix by shipment value

SET @YEAR = 2024;

WITH mode_totals AS (
    SELECT
        f.year,
        f.mode_id,
        SUM(f.value) AS total_value,
        SUM(f.current_value) AS total_current_value,
        SUM(f.tons) AS total_tons,
        SUM(f.tmiles) AS total_tmiles
    FROM fact_faf AS f
    WHERE f.year = @YEAR
    GROUP BY
        f.year,
        f.mode_id
),
grand_total AS (
    SELECT
        SUM(total_value) AS grand_total_value
    FROM mode_totals
)
SELECT
    mt.year,
    m.mode_name,
    mt.total_value,
    mt.total_current_value,
    mt.total_tons,
    mt.total_tmiles,
    ROUND((mt.total_value / NULLIF(gt.grand_total_value, 0)) * 100, 2) AS value_share_pct
FROM mode_totals AS mt
JOIN dim_mode AS m
    ON mt.mode_id = m.mode_id
CROSS JOIN grand_total AS gt
ORDER BY mt.total_value DESC;