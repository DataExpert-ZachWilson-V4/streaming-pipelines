CREATE OR REPLACE TABLE bhakti.spark_streaming_session_homework
(
	session_id INTEGER,
	window_start TIMESTAMP(6) WITH TIME ZONE,
	window_end TIMESTAMP(6) WITH TIME ZONE,
	event_count BIGINT,
	session_start_date DATE,
	country VARCHAR,
	state VARCHAR,
	city VARCHAR,
	os VARCHAR,
	browser VARCHAR,
	logged_in_or_out VARCHAR
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_start_date']


  CREATE TABLE session_tracking (
  session_id VARCHAR,
  session_start TIMESTAMP,
  session_end TIMESTAMP,
  event_count BIGINT,
  session_date DATE,
  city VARCHAR,
  country VARCHAR,
  state VARCHAR,
  operating_system VARCHAR,
  browser VARCHAR,
  logged_in BOOLEAN
);