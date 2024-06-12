-- session_ddl.sql
--
--
-- Write a DDL query (session_ddl.sql) that creates a table that tracks Data Expert sessions.


-- Create or replace the  table to track DataExpert.io sessions
CREATE OR REPLACE TABLE jlcharbneau.dataexpert_sessions (
    -- Unique identifier for each session
    session_id VARCHAR,

    -- Start timestamp of the session
    window_start TIMESTAMP,

    -- End timestamp of the session
    window_end TIMESTAMP,

    -- Count of events occurred during the session
    event_count BIGINT,

    -- The date when the session started
    session_start_date DATE,

    -- City associated with the session
    city VARCHAR,

    -- State associated with the session
    state VARCHAR,

    -- Country associated with the session
    country VARCHAR,

    -- Operating system used during the session
    os VARCHAR,

    -- Browser used during the session
    browser VARCHAR,

    -- Session state indicating whether the user is logged in or out
    logged_in_or_out VARCHAR
)
WITH (
    -- Parquet format
    format = 'PARQUET',
    -- Declares partitioning of the table by session_start_date for optimized queries
    partitioning = ARRAY['session_start_date']
)