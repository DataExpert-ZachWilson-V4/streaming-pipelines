CREATE OR REPLACE TABLE sanniepatron.spark_streaming_session_homework
(
    session_id VARCHAR not null, --'Unique identifier for the session'
    user_id VARCHAR, --'Identifier for the user'
    ip VARCHAR, --'IP address of the user during the session'
    window_start TIMESTAMP(6), --'Start time of the session window'
    window_end TIMESTAMP(6),  --'End time of the session window'
    event_count BIGINT, --'Number of events in the session'
    session_start_date DATE, --'Date when the session started'
    country VARCHAR, --'Country from which the session originated'
    state VARCHAR, --'State from which the session originated'
    city VARCHAR, --'City from which the session originated'
    os VARCHAR, --'Device family (OS) used during the session'
    browser VARCHAR, -- 'Browser family used during the session'
    logged_in_or_out BOOLEAN --'User logged in status during the session'
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_start_date']
)