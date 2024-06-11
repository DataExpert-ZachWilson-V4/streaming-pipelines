-- DDL for creating spark_streaming_session_homework table
CREATE OR REPLACE TABLE billyswitzer.spark_streaming_session_homework
(
    -- Unique identifier for each session
    session_id INTEGER,

    -- Start timestamp of the session with time zone information
    window_start TIMESTAMP(6) WITH TIME ZONE,

    -- End timestamp of the session with time zone information
    window_end TIMESTAMP(6) WITH TIME ZONE,

    -- Count of events occurred during the session
    event_count BIGINT,

    -- The date when the session started
    session_start_date DATE,

    -- Country associated with the session
    country VARCHAR,

    -- State associated with the session
    state VARCHAR,

    -- City associated with the session
    city VARCHAR,

    -- Operating system used in the session
    os VARCHAR,

    -- Browser used during the session
    browser VARCHAR,

    -- Session state whether the user is logged in or out
    logged_in_or_out VARCHAR
)
WITH 
(
    -- Specifies the file format of the table
    format = 'PARQUET',

    -- Declares partitioning of the table by session_start_date for optimized queries
    partitioning = ARRAY['session_start_date']
)