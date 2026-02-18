USE faf_dw;

SELECT
  'origin_zone_id -> dim_zone' AS check_name,
  COUNT(*) AS orphan_id_count
FROM (
  SELECT origin_zone_id AS id
  FROM fact_faf FORCE INDEX (ix_fact_origin_zone_id)
  WHERE origin_zone_id IS NOT NULL
  GROUP BY origin_zone_id
) fk
LEFT JOIN dim_zone z ON z.zone_id = fk.id
WHERE z.zone_id IS NULL;

-- ---------------------------
-- dim_zone (destination)
-- ---------------------------
SELECT
  'destination_zone_id -> dim_zone' AS check_name,
  COUNT(*) AS orphan_id_count
FROM (
  SELECT destination_zone_id AS id
  FROM fact_faf FORCE INDEX (ix_fact_destination_zone_id)
  WHERE destination_zone_id IS NOT NULL
  GROUP BY destination_zone_id
) fk
LEFT JOIN dim_zone z ON z.zone_id = fk.id
WHERE z.zone_id IS NULL;

-- ---------------------------
-- dim_mode
-- ---------------------------
SELECT
  'mode_id -> dim_mode' AS check_name,
  COUNT(*) AS orphan_id_count
FROM (
  SELECT mode_id AS id
  FROM fact_faf FORCE INDEX (ix_fact_mode_id)
  WHERE mode_id IS NOT NULL
  GROUP BY mode_id
) fk
LEFT JOIN dim_mode m ON m.mode_id = fk.id
WHERE m.mode_id IS NULL;

-- ---------------------------
-- dim_commodity
-- ---------------------------
SELECT
  'commodity_id -> dim_commodity' AS check_name,
  COUNT(*) AS orphan_id_count
FROM (
  SELECT commodity_id AS id
  FROM fact_faf FORCE INDEX (ix_fact_commodity_id)
  WHERE commodity_id IS NOT NULL
  GROUP BY commodity_id
) fk
LEFT JOIN dim_commodity c ON c.commodity_id = fk.id
WHERE c.commodity_id IS NULL;

-- ---------------------------
-- dim_trade_type
-- ---------------------------
SELECT
  'trade_type_id -> dim_trade_type' AS check_name,
  COUNT(*) AS orphan_id_count
FROM (
  SELECT trade_type_id AS id
  FROM fact_faf FORCE INDEX (ix_fact_trade_type_id)
  WHERE trade_type_id IS NOT NULL
  GROUP BY trade_type_id
) fk
LEFT JOIN dim_trade_type t ON t.trade_type_id = fk.id
WHERE t.trade_type_id IS NULL;

-- ---------------------------
-- dim_distance_band
-- ---------------------------
SELECT
  'dist_band_id -> dim_distance_band' AS check_name,
  COUNT(*) AS orphan_id_count
FROM (
  SELECT dist_band_id AS id
  FROM fact_faf FORCE INDEX (ix_fact_dist_band_id)
  WHERE dist_band_id IS NOT NULL
  GROUP BY dist_band_id
) fk
LEFT JOIN dim_distance_band d ON d.dist_band_id = fk.id
WHERE d.dist_band_id IS NULL;
