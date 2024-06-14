CREATE OR REPLACE TABLE phabrahao.sessions (
    is_logged_user INTEGER,
    session_start_date DATE,
    session_start_ts TIMESTAMP(6) WITH TIME ZONE,
    session_end_ts TIMESTAMP(6) WITH TIME ZONE,
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