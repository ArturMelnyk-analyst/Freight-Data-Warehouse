USE faf_dw;

SELECT
  'origin_both_dms_and_fr_filled' AS check_name,
  COUNT(*) AS failed_rows
FROM stg_faf
WHERE dms_orig IS NOT NULL
  AND fr_orig IS NOT NULL

UNION ALL

SELECT
  'destination_both_dms_and_fr_filled' AS check_name,
  COUNT(*) AS failed_rows
FROM stg_faf
WHERE dms_dest IS NOT NULL
  AND fr_dest IS NOT NULL;
