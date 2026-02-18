USE faf_dw;

SELECT
  'negative_measures' AS check_name,
  COUNT(*) AS failed_rows
FROM fact_faf
WHERE (tons < 0)
   OR (value < 0)
   OR (current_value < 0)
   OR (tmiles < 0);
