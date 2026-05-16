-- Tableau Export 04
-- Distance-band distribution by shipment value, tons, and ton-miles

SET @YEAR = 2024;

WITH distance_totals AS (
    SELECT
        f.year,
        f.dist_band_id,
        SUM(f.value) AS total_value,
        SUM(f.current_value) AS total_current_value,
        SUM(f.tons) AS total_tons,
        SUM(f.tmiles) AS total_tmiles
    FROM fact_faf AS f
    WHERE f.year = @YEAR
    GROUP BY
        f.year,
        f.dist_band_id
),
grand_total AS (
    SELECT
        SUM(total_value) AS grand_total_value
    FROM distance_totals
)
SELECT
    dt.year,
    db.dist_band_code,
    db.dist_band_name,
    dt.total_value,
    dt.total_current_value,
    dt.total_tons,
    dt.total_tmiles,
    ROUND((dt.total_value / NULLIF(gt.grand_total_value, 0)) * 100, 2) AS value_share_pct
FROM distance_totals AS dt
JOIN dim_distance_band AS db
    ON dt.dist_band_id = db.dist_band_id
CROSS JOIN grand_total AS gt
ORDER BY db.dist_band_code;