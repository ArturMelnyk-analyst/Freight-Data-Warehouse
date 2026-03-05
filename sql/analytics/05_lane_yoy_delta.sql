USE faf_dw;
SET @Y1 := 2023;
SET @Y2 := 2024;

WITH lanes AS (
  SELECT origin_zone_id, destination_zone_id
  FROM fact_faf
  WHERE year IN (@Y1, @Y2)
  GROUP BY origin_zone_id, destination_zone_id
),
agg AS (
  SELECT
    f.year,
    f.origin_zone_id,
    f.destination_zone_id,
    SUM(f.value) AS total_value
  FROM fact_faf f
  JOIN lanes l
    ON l.origin_zone_id = f.origin_zone_id
   AND l.destination_zone_id = f.destination_zone_id
  WHERE f.year IN (@Y1, @Y2)
  GROUP BY f.year, f.origin_zone_id, f.destination_zone_id
)
SELECT
  a2.origin_zone_id,
  a2.destination_zone_id,
  a1.total_value AS value_y1,
  a2.total_value AS value_y2,
  (a2.total_value - a1.total_value) AS delta_value,
  ROUND(100 * (a2.total_value - a1.total_value) / NULLIF(a1.total_value, 0), 2) AS pct_change
FROM agg a2
JOIN agg a1
  ON a1.origin_zone_id = a2.origin_zone_id
 AND a1.destination_zone_id = a2.destination_zone_id
WHERE a1.year = @Y1 AND a2.year = @Y2
ORDER BY delta_value DESC
LIMIT 20;