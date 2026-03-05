USE faf_dw;
SET @YEAR := 2024;

SELECT
  t.trade_type_name,
  SUM(f.value) AS total_value,
  ROUND(100 * SUM(f.value) / SUM(SUM(f.value)) OVER (), 2) AS pct_value
FROM fact_faf f
JOIN dim_trade_type t ON t.trade_type_id = f.trade_type_id
WHERE f.year = @YEAR
GROUP BY t.trade_type_name
ORDER BY total_value DESC;