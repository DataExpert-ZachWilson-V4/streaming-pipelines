CREATE OR REPLACE TABLE tejalscr.spark_streaming_session_hw6
(
  session_id INT,
  window_start TIMESTAMP(6) with TIME ZONE,
  window_end TIMESTAMP(6) with TIME ZONE,
  event_count BIGINT,
  session_start_date DATE,
  city VARCHAR,
  country VARCHAR,
  state VARCHAR,
  os VARCHAR,
  browser VARCHAR,
  Logged_in_out_flag BOOLEAN
)
WITH 
(   format = 'PARQUET',
    partitioning = ARRAY['session_start_date']
)