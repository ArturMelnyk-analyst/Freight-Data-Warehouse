-- Tableau Export 01
-- Top freight corridors by shipment value

SET @YEAR = 2024;

WITH corridor_totals AS (
    SELECT
        f.year,
        f.origin_zone_id,
        f.destination_zone_id,
        SUM(f.value) AS total_value,
        SUM(f.current_value) AS total_current_value,
        SUM(f.tons) AS total_tons,
        SUM(f.tmiles) AS total_tmiles
    FROM fact_faf AS f
    WHERE f.year = @YEAR
    GROUP BY
        f.year,
        f.origin_zone_id,
        f.destination_zone_id
)
SELECT
    ct.year,
    oz.zone_name AS origin_zone_name,
    dz.zone_name AS destination_zone_name,
    CONCAT(oz.zone_name, ' → ', dz.zone_name) AS freight_corridor,
    ct.total_value,
    ct.total_current_value,
    ct.total_tons,
    ct.total_tmiles
FROM corridor_totals AS ct
JOIN dim_zone AS oz
    ON ct.origin_zone_id = oz.zone_id
JOIN dim_zone AS dz
    ON ct.destination_zone_id = dz.zone_id
ORDER BY ct.total_value DESC
LIMIT 25;