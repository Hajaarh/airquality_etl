CREATE TABLE IF NOT EXISTS airq_ops.pipeline_runs (
  run_id STRING,
  target_date DATE,
  source STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  records_out INT64,
  gcs_raw_path STRING,
  error_message STRING
);
