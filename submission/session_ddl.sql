-- Create a partitioned table for session data
CREATE TABLE IF NOT EXISTS alia.tmp_sessions (
session_id STRING,
session_window_start TIMESTAMP ,
session_window_end TIMESTAMP ,
session_date INT,
country STRING,
state STRING,
city STRING,
os STRING,
browser STRING,
is_user_logged BOOLEAN,
event_count BIGINT
)
USING ICEBERG
PARTITIONED BY (session_date)