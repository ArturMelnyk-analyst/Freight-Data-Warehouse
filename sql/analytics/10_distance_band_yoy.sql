USE faf_dw;
SET @YEAR := 2024;

WITH top_lanes AS (
    SELECT
        f.origin_zone_id,
        f.destination_zone_id,
        SUM(f.value) AS total_value
    FROM fact_faf f FORCE INDEX (ix_perf_year_lane_value)
    WHERE f.year = @YEAR
    GROUP BY f.origin_zone_id, f.destination_zone_id
    ORDER BY total_value DESC
    LIMIT 20
)
SELECT
    @YEAR AS year,
    oz.zone_name AS origin,
    dz.zone_name AS destination,
    tl.total_value
FROM top_lanes tl
JOIN dim_zone oz ON oz.zone_id = tl.origin_zone_id
JOIN dim_zone dz ON dz.zone_id = tl.destination_zone_id
ORDER BY tl.total_value DESC;