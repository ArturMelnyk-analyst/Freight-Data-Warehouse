USE faf_dw;

ALTER TABLE etl_run_log
  ADD COLUMN expected_fact_rows BIGINT NULL,
  ADD COLUMN fact_row_count BIGINT NULL;
