USE faf_dw;

-- FK support indexes (added in PR#5)
-- Required to avoid full scans in validation and analytics joins

-- Already exist:
-- ix_fact_origin_zone_id
-- ix_fact_destination_zone_id
-- ix_fact_mode_id
-- ix_fact_commodity_id
-- ix_fact_trade_type_id
-- ix_fact_dist_band_id
-- ix_fact_year


-- Q1: Lane top-20 inside a year (still heavy, keep as realism benchmark)
SELECT
  f.year,
  f.origin_zone_id,
  f.destination_zone_id,
  SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = 2024
GROUP BY f.year, f.origin_zone_id, f.destination_zone_id
ORDER BY total_value DESC
LIMIT 20;

-- Q2: Mode + commodity slice inside year (realistic dashboard slice)
SELECT
  f.year, f.mode_id, f.commodity_id,
  SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = 2024
  AND f.mode_id = 1
GROUP BY f.year, f.mode_id, f.commodity_id
ORDER BY total_value DESC
LIMIT 20;

-- Q3: Trade type slice inside year
SELECT
  f.year, f.trade_type_id,
  SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = 2024
  AND f.trade_type_id = 1
GROUP BY f.year, f.trade_type_id
ORDER BY total_value DESC
LIMIT 20;

-- Q4: Distance band slice inside year
SELECT
  f.year, f.dist_band_id,
  SUM(f.tons) AS total_tons
FROM fact_faf f
WHERE f.year = 2024
GROUP BY f.year, f.dist_band_id
ORDER BY total_tons DESC
LIMIT 20;

-- Q5: join with dim_zone + filter origin (more selective join case)
SELECT
  f.year,
  oz.zone_name AS origin,
  dz.zone_name AS destination,
  SUM(f.value) AS total_value
FROM fact_faf f
JOIN dim_zone oz ON oz.zone_id = f.origin_zone_id
JOIN dim_zone dz ON dz.zone_id = f.destination_zone_id
WHERE f.year = 2024
  AND f.origin_zone_id = 9
GROUP BY f.year, oz.zone_name, dz.zone_name
ORDER BY total_value DESC
LIMIT 20;