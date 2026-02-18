USE faf_dw;

SELECT
  'null_dimension_keys' AS check_name,
  COUNT(*) AS failed_rows
FROM fact_faf
WHERE origin_zone_id IS NULL
   OR destination_zone_id IS NULL
   OR mode_id IS NULL
   OR commodity_id IS NULL
   OR trade_type_id IS NULL
   OR dist_band_id IS NULL
   OR year IS NULL;
