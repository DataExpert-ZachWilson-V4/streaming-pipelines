CREATE OR REPLACE TABLE erich.dataexpert_sessions (
    session_id INTEGER,
    is_logged_user INTEGER,
    session_start_date DATE,
    session_start_ts TIMESTAMP,
    session_end_ts TIMESTAMP,
    event_count BIGINT,
    country VARCHAR,
    city VARCHAR,
    state VARCHAR,
    os VARCHAR,
    browser VARCHAR
)
WITH 
(
    partitioning = ARRAY['session_start_date']
)