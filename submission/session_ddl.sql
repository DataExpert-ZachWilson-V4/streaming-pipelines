-- Table contains aggregates for each user web session

CREATE OR REPLACE TABLE ovoxo.spark_streaming_data
(
	session_id INTEGER,
	window_start_ts TIMESTAMP,
	window_end_ts TIMESTAMP,
	event_count BIGINT,
	session_start_date DATE,
	country VARCHAR,
	state VARCHAR,
	city VARCHAR,
	os VARCHAR,
	browser VARCHAR,
	is_logged_in BOOLEAN
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_start_date']
)
