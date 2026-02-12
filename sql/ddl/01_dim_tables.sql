USE faf_dw;

-- -----------------------
-- Dimension: Zone (unified domestic + foreign)
-- -----------------------
CREATE TABLE IF NOT EXISTS dim_zone (
  zone_id INT AUTO_INCREMENT PRIMARY KEY,
  zone_type VARCHAR(10) NOT NULL,      -- 'DMS' or 'FR'
  zone_code VARCHAR(32) NOT NULL,      -- original code from metadata
  zone_name VARCHAR(255) NULL,
  UNIQUE KEY uk_zone (zone_type, zone_code)
);

-- -----------------------
-- Dimension: Mode
-- -----------------------
CREATE TABLE IF NOT EXISTS dim_mode (
  mode_id INT AUTO_INCREMENT PRIMARY KEY,
  mode_code VARCHAR(32) NOT NULL,
  mode_name VARCHAR(255) NULL,
  UNIQUE KEY uk_mode (mode_code)
);

-- -----------------------
-- Dimension: Commodity (SCTG2)
-- -----------------------
CREATE TABLE IF NOT EXISTS dim_commodity (
  commodity_id INT AUTO_INCREMENT PRIMARY KEY,
  sctg2 VARCHAR(32) NOT NULL,
  commodity_name VARCHAR(255) NULL,
  UNIQUE KEY uk_sctg2 (sctg2)
);

-- -----------------------
-- Dimension: Trade Type
-- -----------------------
CREATE TABLE IF NOT EXISTS dim_trade_type (
  trade_type_id INT AUTO_INCREMENT PRIMARY KEY,
  trade_type_code VARCHAR(32) NOT NULL,
  trade_type_name VARCHAR(255) NULL,
  UNIQUE KEY uk_trade_type (trade_type_code)
);

-- -----------------------
-- Dimension: Distance Band
-- -----------------------
CREATE TABLE IF NOT EXISTS dim_distance_band (
  dist_band_id INT AUTO_INCREMENT PRIMARY KEY,
  dist_band_code VARCHAR(32) NOT NULL,
  dist_band_name VARCHAR(255) NULL,
  UNIQUE KEY uk_dist_band (dist_band_code)
);

-- -----------------------
-- Dimension: Year
-- -----------------------
CREATE TABLE IF NOT EXISTS dim_year (
  year INT PRIMARY KEY
);
