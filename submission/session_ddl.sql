CREATE OR REPLACE TABLE mposada.dataexpert_sessions (
    host STRING,
    session_id STRING,
    user_id BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_date DATE,
    event_count BIGINT,
    country STRING,
    state STRING,
    city STRING,
    browser_family STRING,
    device_family STRING,
    is_logged_in BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_date)
