CREATE TABLE IF NOT EXISTS sagararora492.streaming_sessions_tracking (
    id STRING,
    country STRING,
    state STRING,
    city STRING,
    operating_system STRING,
    browser STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    session_date DATE,
    n_events INTEGER,
    logged_in BOOLEAN
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_date']
)
USING ICEBERG;