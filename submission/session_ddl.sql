
CREATE OR REPLACE TABLE aayushi.flink_sessions (

    session_id VARCHAR,  -- concat of user_id, ip, window_start
    user_id VARCHAR,
    id VARCHAR,
    country VARCHAR,
    state VARCHAR,
    city VARCHAR,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    event_count BIGINT,
    os_type VARCHAR,
    browser VARCHAR,
    user_session_status VARCHAR

)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['window_start']
)