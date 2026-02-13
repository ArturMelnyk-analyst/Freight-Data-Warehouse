USE faf_dw;

CREATE TABLE IF NOT EXISTS stg_faf (
    stg_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

    dms_orig VARCHAR(20),
    dms_dest VARCHAR(20),
    fr_orig VARCHAR(20),
    fr_dest VARCHAR(20),
    dms_mode VARCHAR(20),
    fr_inmode VARCHAR(20),
    fr_outmode VARCHAR(20),
    sctg2 VARCHAR(20),
    trade_type VARCHAR(20),
    dist_band VARCHAR(20),

    tons_2018 DOUBLE,
    value_2018 DOUBLE,
    current_value_2018 DOUBLE,
    tmiles_2018 DOUBLE,

    tons_2019 DOUBLE,
    value_2019 DOUBLE,
    current_value_2019 DOUBLE,
    tmiles_2019 DOUBLE,

    tons_2020 DOUBLE,
    value_2020 DOUBLE,
    current_value_2020 DOUBLE,
    tmiles_2020 DOUBLE,

    tons_2021 DOUBLE,
    value_2021 DOUBLE,
    current_value_2021 DOUBLE,
    tmiles_2021 DOUBLE,

    tons_2022 DOUBLE,
    value_2022 DOUBLE,
    current_value_2022 DOUBLE,
    tmiles_2022 DOUBLE,

    tons_2023 DOUBLE,
    value_2023 DOUBLE,
    current_value_2023 DOUBLE,
    tmiles_2023 DOUBLE,

    tons_2024 DOUBLE,
    value_2024 DOUBLE,
    current_value_2024 DOUBLE,
    tmiles_2024 DOUBLE
);
