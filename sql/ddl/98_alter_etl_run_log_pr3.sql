USE faf_dw;

ALTER TABLE etl_run_log
  ADD COLUMN source_file        VARCHAR(255) NULL,
  ADD COLUMN file_hash          CHAR(32)     NULL,
  ADD COLUMN file_modified_ts   DATETIME    NULL,
  ADD COLUMN stg_row_count      BIGINT      NULL;
