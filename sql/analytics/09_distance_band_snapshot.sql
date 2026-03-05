USE faf_dw;
SET @YEAR := 2024;

WITH agg AS (
  SELECT
    f.dist_band_id,
    SUM(f.tons)   AS total_tons,
    SUM(f.tmiles) AS total_tmiles
  FROM fact_faf f FORCE INDEX (ix_perf_year_dist_metrics)
  WHERE f.year = @YEAR
  GROUP BY f.dist_band_id
)
SELECT
  @YEAR AS year,
  d.dist_band_name,
  agg.total_tons,
  agg.total_tmiles
FROM agg
JOIN dim_distance_band d
  ON d.dist_band_id = agg.dist_band_id
ORDER BY agg.total_tmiles DESC;

