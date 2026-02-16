USE faf_dw;

ALTER TABLE etl_run_log
  MODIFY COLUMN notes LONGTEXT;
