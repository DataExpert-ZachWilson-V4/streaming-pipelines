--DDL query that creates table to track the DataExpert sessions

CREATE TABLE hdamerla.session_data (
  user_id BIGINT,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  session_id STRING,
  event_count BIGINT,
  session_date DATE,
  city STRING,
  state STRING,
  country STRING,
  device_family STRING,
  browser_family STRING,
  is_logged_in BOOLEAN
  )
  WITH
  (
    format = 'PARQUET',
    partitioning = ARRAY['session_date']
    )
  
