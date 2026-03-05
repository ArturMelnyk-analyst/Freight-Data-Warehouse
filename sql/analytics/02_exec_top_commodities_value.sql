USE faf_dw;
SET @YEAR := 2024;

SELECT
  f.year,
  c.commodity_name,
  SUM(f.value) AS total_value
FROM fact_faf f
JOIN dim_commodity c ON c.commodity_id = f.commodity_id
WHERE f.year = @YEAR
GROUP BY f.year, c.commodity_name
ORDER BY total_value DESC
LIMIT 20;