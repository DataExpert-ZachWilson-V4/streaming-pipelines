CREATE TABLE IF NOT EXISTS academy.siawayforward.user_session (
  session_id STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  event_count INTEGER,
  session_start_date DATE,
  user_city STRING,
  user_state STRING,
  user_country STRING,
  user_os STRING,
  user_browser STRING,
  is_logged_in BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_start_date)