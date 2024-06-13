CREATE OR REPLACE TABLE adbeyer.spark_streaming_session_homework (
    session_id VARCHAR,
    user_id VARCHAR,
    ip VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    os VARCHAR,
    browser VARCHAR,
    session_start TIMESTAMP(6) WITH TIME ZONE,
    session_end TIMESTAMP(6) WITH TIME ZONE,
    session_date DATE,
    event_count BIGINT,
    logged_in BOOLEAN
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_date']
)