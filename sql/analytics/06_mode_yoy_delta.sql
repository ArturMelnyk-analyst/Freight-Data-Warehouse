USE faf_dw;
SET @Y1 := 2023;
SET @Y2 := 2024;

WITH agg AS (
  SELECT
    year,
    mode_id,
    SUM(value) AS total_value
  FROM fact_faf
  WHERE year IN (@Y1, @Y2)
  GROUP BY year, mode_id
)
SELECT
  m.mode_name,
  a1.total_value AS value_y1,
  a2.total_value AS value_y2,
  (a2.total_value - a1.total_value) AS delta_value,
  ROUND(100 * (a2.total_value - a1.total_value) / NULLIF(a1.total_value, 0), 2) AS pct_change
FROM agg a1
JOIN agg a2 ON a2.mode_id = a1.mode_id
JOIN dim_mode m ON m.mode_id = a1.mode_id
WHERE a1.year = @Y1 AND a2.year = @Y2
ORDER BY delta_value DESC;