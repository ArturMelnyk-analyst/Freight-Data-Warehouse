USE faf_dw;

-- Covering index for lane-by-year value aggregation
CREATE INDEX ix_perf_year_lane_value
ON fact_faf (year, origin_zone_id, destination_zone_id, value);

-- Covering index for mode+commodity slices (value)
CREATE INDEX ix_perf_year_mode_comm_value
ON fact_faf (year, mode_id, commodity_id, value);

-- Covering index for trade_type slice (value)
CREATE INDEX ix_perf_year_trade_value
ON fact_faf (year, trade_type_id, value);

-- Covering index for dist_band slice (tons)
CREATE INDEX ix_perf_year_dist_tons
ON fact_faf (year, dist_band_id, tons);