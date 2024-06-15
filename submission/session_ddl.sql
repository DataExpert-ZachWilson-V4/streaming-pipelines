-- DDL for creating spark_streaming_session_homework table
CREATE OR REPLACE TABLE grisreyesrios.spark_session_streaming
(
    -- Unique identifier 
    session_id INTEGER PRIMARY KEY,

    -- Start timestamp of the session 
    window_start TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- End timestamp of the session 
    window_end TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    
    -- Count of events occurred during the session
    event_count BIGINT,

    -- The date when the session starts
    session_start_date DATE,

    
    country VARCHAR,

    
    state VARCHAR,

    
    city VARCHAR,

    -- Operating system used 
    os VARCHAR,

    -- Browser 
    browser VARCHAR,

    -- Session state whether the user is logged in or out
    logged_in_or_out VARCHAR
)
WITH 
(
    -- file format 
    format = 'PARQUET',

    -- Declares partitioning 
    partitioning = ARRAY['session_start_date']
)