USE faf_dw;
SET @YEAR := 2024;

WITH outflow AS (
  SELECT origin_zone_id AS zone_id, SUM(value) AS out_value
  FROM fact_faf
  WHERE year = @YEAR
  GROUP BY origin_zone_id
),
inflow AS (
  SELECT destination_zone_id AS zone_id, SUM(value) AS in_value
  FROM fact_faf
  WHERE year = @YEAR
  GROUP BY destination_zone_id
)
SELECT
  z.zone_name,
  COALESCE(i.in_value, 0) AS inbound_value,
  COALESCE(o.out_value, 0) AS outbound_value,
  (COALESCE(i.in_value, 0) - COALESCE(o.out_value, 0)) AS net_value
FROM dim_zone z
LEFT JOIN inflow i ON i.zone_id = z.zone_id
LEFT JOIN outflow o ON o.zone_id = z.zone_id
ORDER BY net_value DESC
LIMIT 20;