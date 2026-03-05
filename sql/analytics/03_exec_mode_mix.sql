USE faf_dw;
SET @YEAR := 2024;

SELECT
  f.year,
  m.mode_name,
  SUM(f.value) AS total_value,
  ROUND(100 * SUM(f.value) / SUM(SUM(f.value)) OVER (), 2) AS pct_value
FROM fact_faf f
JOIN dim_mode m ON m.mode_id = f.mode_id
WHERE f.year = @YEAR
GROUP BY f.year, m.mode_name
ORDER BY total_value DESC;