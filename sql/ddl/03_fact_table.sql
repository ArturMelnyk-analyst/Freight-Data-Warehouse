USE faf_dw;

CREATE TABLE IF NOT EXISTS fact_faf (
    fact_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

    origin_zone_id BIGINT UNSIGNED NOT NULL,
    destination_zone_id BIGINT UNSIGNED NOT NULL,
    mode_id BIGINT UNSIGNED NOT NULL,
    commodity_id BIGINT UNSIGNED NOT NULL,
    trade_type_id BIGINT UNSIGNED NOT NULL,
    dist_band_id BIGINT UNSIGNED NOT NULL,
    year SMALLINT NOT NULL,

    tons DOUBLE NULL,
    value DOUBLE NULL,
    current_value DOUBLE NULL,
    tmiles DOUBLE NULL,

    -- prevent duplicates at final grain
    UNIQUE KEY uk_fact_grain (
        origin_zone_id, destination_zone_id,
        mode_id, commodity_id, trade_type_id, dist_band_id,
        year
    )
);
