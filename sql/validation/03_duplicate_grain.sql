USE faf_dw;

SELECT
  'uk_fact_grain_exists_and_unique' AS check_name,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.statistics
      WHERE table_schema = DATABASE()
        AND table_name = 'fact_faf'
        AND index_name = 'uk_fact_grain'
      GROUP BY index_name
      HAVING MAX(non_unique) = 0
    )
    THEN 0
    ELSE 1
  END AS failed_rows;

