-- Table to track DataExpert.io sessions
CREATE OR REPLACE TABLE supreethkabbin.dataexpert_sessions (
    -- Unique identifier for each session
    session_id VARCHAR,
    -- Start timestamp of the session
    window_start TIMESTAMP,
    -- End timestamp of the session
    window_end TIMESTAMP,
    -- Count of events during the session
    event_count BIGINT,
    -- The date when the session started
    session_date DATE,
    -- City associated with the session
    city VARCHAR,
    -- State associated with the session
    state VARCHAR,
    -- Country associated with the session
    country VARCHAR,
    -- Operating system associated with the session
    device_family VARCHAR,
    -- Browser associated with the session
    browser_family VARCHAR,
    -- Session state indicating whether the user is logged in or out
    is_logged_in BOOLEAN,
    -- Unique identifier for the user
    user_id VARCHAR,
    -- Host associated with the session
    host VARCHAR
) WITH (
    partitioning = ARRAY['session_date']
)