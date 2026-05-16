-- Tableau support indexes
-- Purpose: speed up small dashboard export queries.
-- These are intentionally aligned with the Tableau CSV export layer.

CREATE INDEX ix_tableau_year_lane_measures
ON fact_faf (
    year,
    origin_zone_id,
    destination_zone_id,
    value,
    current_value,
    tons,
    tmiles
);

CREATE INDEX ix_tableau_year_mode_measures
ON fact_faf (
    year,
    mode_id,
    value,
    current_value,
    tons,
    tmiles
);

CREATE INDEX ix_tableau_year_commodity_measures
ON fact_faf (
    year,
    commodity_id,
    value,
    current_value,
    tons,
    tmiles
);

CREATE INDEX ix_tableau_year_dist_measures
ON fact_faf (
    year,
    dist_band_id,
    value,
    current_value,
    tons,
    tmiles
);