--The spark_streaming_session_homework table stores web event counts per user session
--  session_id - a unique identifier for the session, hashed from the user_id, ip, and window_start
--  window_start / window_end - the start and end of the session
--  event_count - how many events happened in that session
--  session_start_date - the date of the beginning of the session
--  city / country / state - What city, country, and state is associated with this session
--  os / browser - What operating system and browser are associated with this session
        -- os and browser are both logged only at the family level, e.g. - Chrome, instead of "Chrome 125.0.6422.142"
--  logged_in_or_out - Whether this session is for logged in or logged out users

CREATE OR REPLACE TABLE billyswitzer.spark_streaming_session_homework
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
)