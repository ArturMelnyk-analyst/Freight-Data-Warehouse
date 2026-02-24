USE faf_dw;

-- Keep optimizer honest and stats fresh
ANALYZE TABLE fact_faf;

EXPLAIN ANALYZE
SELECT f.year, f.origin_zone_id, f.destination_zone_id, SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = 2024
GROUP BY f.year, f.origin_zone_id, f.destination_zone_id
ORDER BY total_value DESC
LIMIT 20;

EXPLAIN ANALYZE
SELECT f.year, f.mode_id, f.commodity_id, SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = 2024 AND f.mode_id = 1
GROUP BY f.year, f.mode_id, f.commodity_id
ORDER BY total_value DESC
LIMIT 20;

EXPLAIN ANALYZE
SELECT f.year, f.trade_type_id, SUM(f.value) AS total_value
FROM fact_faf f
WHERE f.year = 2024 AND f.trade_type_id = 1
GROUP BY f.year, f.trade_type_id
ORDER BY total_value DESC
LIMIT 20;

EXPLAIN ANALYZE
SELECT f.year, f.dist_band_id, SUM(f.tons) AS total_tons
FROM fact_faf f
WHERE f.year = 2024
GROUP BY f.year, f.dist_band_id
ORDER BY total_tons DESC
LIMIT 20;

EXPLAIN ANALYZE
SELECT f.year, oz.zone_name AS origin, dz.zone_name AS destination, SUM(f.value) AS total_value
FROM fact_faf f
JOIN dim_zone oz ON oz.zone_id = f.origin_zone_id
JOIN dim_zone dz ON dz.zone_id = f.destination_zone_id
WHERE f.year = 2024 AND f.origin_zone_id = 9
GROUP BY f.year, oz.zone_name, dz.zone_name
ORDER BY total_value DESC
LIMIT 20;