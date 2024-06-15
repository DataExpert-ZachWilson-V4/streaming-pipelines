'''query that creates the table streaming_data:
	window_start_ts: the start of the session
	window_end_ts: the end of the session
	session_id: a unique identifier for the session
	event_count: how many events happened in that session
	session_start: the date of the beginning of the session
	country: what country is associated with this session
	State: what state is associated with this session
	City: what city is associated with this session
	os: what operating system is associated with this session
	browser: what browser is associated with this session
	is_logged_in: whether this session is for logged in or logged out users
'''
CREATE OR REPLACE TABLE ykshon52797255.streaming_data
(
	--a unique identifier for the session
	session_id INTEGER,
	--the start of the session
	window_start_ts TIMESTAMP,
	--the end of the session
	window_end_ts TIMESTAMP,
	--how many events happened in that session
	event_count BIGINT,
	--the date of the beginning of the session
	session_start_date DATE,
	--what country is associated with this session
	country VARCHAR,
	--what state is associated with this session
	state VARCHAR,
	--what city is associated with this session
	city VARCHAR,
	--what operating system is associated with this session
	os VARCHAR,
	--what browser is associated with this session
	browser VARCHAR,
	--whether this session is for logged in or logged out users
	is_logged_in BOOLEAN
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_start_date']
)
