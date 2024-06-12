CREATE TABLE IF NOT EXISTS meetapandit89096646.kafka_streaming_session_example ( -- table to store kafka events 
  -- session_id is created from concatenating user_id, ip and window_start timestamp
  session_id VARCHAR,
  user_id VARCHAR,
  -- get ip, country, state and city from ip_location API
  ip VARCHAR,
  country VARCHAR,
  state VARCHAR,
  city VARCHAR,
  -- computed from session_window function and by looking at 5 min windows of sesion start and end
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_count BIGINT,
  -- os_type and browser is extracted from user_agent field from kafka message
  os_type VARCHAR,
  browser VARCHAR,
  -- calculated from user_id column by checkcing whether it is null or not
  user_logged_in_status VARCHAR
-- format of the table stored in Trino and partitioning column
)WITH (
   format = 'PARQUET',
   partitioning = ARRAY['window_start']	
)
