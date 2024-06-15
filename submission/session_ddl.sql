CREATE TABLE IF NOT EXISTS alia.tmp_sessions (
  session_id STRING,
  session_window_start STRING,
  session_window_end STRING,
  session_window_start_date STRING,
  country STRING,
  state STRING,
  city STRING,
  os STRING,
  browser STRING,
  is_user_logged Boolean,
  event_count BIGINT
)
USING ICEBERG