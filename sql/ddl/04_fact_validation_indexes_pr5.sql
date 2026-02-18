USE faf_dw;

-- Create indexes only if they don't already exist (MySQL-safe).
DROP PROCEDURE IF EXISTS add_index_if_missing;
DELIMITER $$

CREATE PROCEDURE add_index_if_missing(
    IN p_table VARCHAR(64),
    IN p_index VARCHAR(64),
    IN p_sql   TEXT
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = p_table
          AND index_name = p_index
        LIMIT 1
    ) THEN
        SET @s = p_sql;
        PREPARE stmt FROM @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$

DELIMITER ;

-- destination already exists in your DB, but this is safe (will skip if present)
CALL add_index_if_missing('fact_faf', 'ix_fact_destination_zone_id',
  'CREATE INDEX ix_fact_destination_zone_id ON fact_faf(destination_zone_id)');

CALL add_index_if_missing('fact_faf', 'ix_fact_origin_zone_id',
  'CREATE INDEX ix_fact_origin_zone_id ON fact_faf(origin_zone_id)');

CALL add_index_if_missing('fact_faf', 'ix_fact_mode_id',
  'CREATE INDEX ix_fact_mode_id ON fact_faf(mode_id)');

CALL add_index_if_missing('fact_faf', 'ix_fact_commodity_id',
  'CREATE INDEX ix_fact_commodity_id ON fact_faf(commodity_id)');

CALL add_index_if_missing('fact_faf', 'ix_fact_trade_type_id',
  'CREATE INDEX ix_fact_trade_type_id ON fact_faf(trade_type_id)');

CALL add_index_if_missing('fact_faf', 'ix_fact_dist_band_id',
  'CREATE INDEX ix_fact_dist_band_id ON fact_faf(dist_band_id)');

CALL add_index_if_missing('fact_faf', 'ix_fact_year',
  'CREATE INDEX ix_fact_year ON fact_faf(year)');

-- optional cleanup (leave it, it's fine)
DROP PROCEDURE IF EXISTS add_index_if_missing;
