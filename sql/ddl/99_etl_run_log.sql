USE faf_dw;

CREATE TABLE IF NOT EXISTS etl_run_log (
  run_id CHAR(36) PRIMARY KEY,
  phase VARCHAR(50) NOT NULL,          -- e.g., 'dimensions'
  status VARCHAR(20) NOT NULL,         -- 'STARTED', 'SUCCESS', 'FAILED'
  start_ts DATETIME NOT NULL,
  end_ts DATETIME NULL,

  -- optional metadata
  notes TEXT NULL,

  -- counts (simple approach)
  dim_zone_count INT NULL,
  dim_mode_count INT NULL,
  dim_commodity_count INT NULL,
  dim_trade_type_count INT NULL,
  dim_distance_band_count INT NULL,
  dim_year_count INT NULL
);
