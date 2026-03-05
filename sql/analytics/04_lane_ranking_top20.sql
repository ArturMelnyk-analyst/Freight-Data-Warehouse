USE faf_dw;
SET @YEAR := 2024;

SELECT
  f.year,
  f.origin_zone_id,
  f.destination_zone_id,
  SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = @YEAR
GROUP BY f.year, f.origin_zone_id, f.destination_zone_id
ORDER BY total_value DESC
LIMIT 20;