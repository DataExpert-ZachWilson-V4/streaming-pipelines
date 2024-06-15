CREATE OR REPLACE TABLE lsleena.session_activity (
    session_id VARCHAR, -- unique session identifier session
    session_start_time TIMESTAMP, -- session start time
    session_end_time TIMESTAMP, -- session end time
    operating_system VARCHAR, -- type of os
    browser_type VARCHAR, -- browser types
    event_count BIGINT, -- number of events
    session_start DATE, -- session start date
    city VARCHAR, -- session logged in city
    state VARCHAR, -- session logged in state
    country VARCHAR, -- session logged in country
    logged_in BOOLEAN -- session user is logged in or logged out
)
with (FORMAT = 'PARQUET', partitioning = ARRAY['session_start'])
