CREATE TABLE IF NOT EXISTS meetapandit89096646.kafka_streaming_session_example (
  session_id VARCHAR,
  user_id VARCHAR,
  ip VARCHAR,
  country VARCHAR,
  state VARCHAR,
  city VARCHAR,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_count BIGINT,
  os_type VARCHAR,
  browser VARCHAR,
  user_logged_in_status VARCHAR
)WITH (
   format = 'PARQUET',
   partitioning = ARRAY['window_start']	
)
