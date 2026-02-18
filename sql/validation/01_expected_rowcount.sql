USE faf_dw;

SELECT
  'expected_rowcount' AS check_name,
  CASE
    WHEN (SELECT COUNT(*) FROM stg_faf) = 0 THEN 1
    WHEN (SELECT COUNT(*) FROM fact_faf) = 0 THEN 1
    ELSE 0
  END AS failed_rows;

