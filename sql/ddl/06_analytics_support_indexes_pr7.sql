USE faf_dw;

-- PR#7: Analytics support index
-- Purpose: make Query 9 (distance band snapshot by year) faster via covering index
-- so MySQL can do index-only reads for tons + tmiles aggregation.

CREATE INDEX ix_perf_year_dist_metrics
ON fact_faf (year, dist_band_id, tons, tmiles);
