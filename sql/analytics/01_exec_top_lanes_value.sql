USE faf_dw;
SET @YEAR := 2024;

SELECT
  f.year,
  oz.zone_name AS origin,
  dz.zone_name AS destination,
  SUM(f.value) AS total_value
FROM fact_faf f
JOIN dim_zone oz ON oz.zone_id = f.origin_zone_id
JOIN dim_zone dz ON dz.zone_id = f.destination_zone_id
WHERE f.year = @YEAR
GROUP BY f.year, oz.zone_name, dz.zone_name
ORDER BY total_value DESC
LIMIT 20;