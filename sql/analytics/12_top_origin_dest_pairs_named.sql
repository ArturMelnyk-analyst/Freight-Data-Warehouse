USE faf_dw;
SET @YEAR := 2024;

WITH agg AS (
  SELECT
    origin_zone_id,
    destination_zone_id,
    SUM(value) AS total_value
  FROM fact_faf
  WHERE year = @YEAR
  GROUP BY origin_zone_id, destination_zone_id
)
SELECT
  oz.zone_name AS origin,
  dz.zone_name AS destination,
  agg.total_value
FROM agg
JOIN dim_zone oz ON oz.zone_id = agg.origin_zone_id
JOIN dim_zone dz ON dz.zone_id = agg.destination_zone_id
ORDER BY agg.total_value DESC
LIMIT 20;