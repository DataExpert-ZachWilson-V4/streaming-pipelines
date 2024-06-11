CREATE TABLE IF NOT EXISTS mposada.dataexpert_sessions (
    session_id STRING,
    user_id BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_date DATE,
    event_count BIGINT,
    country STRING,
    state STRING,
    city STRING,
    browser STRING,
    os STRING,
    is_logged_in BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_date)
