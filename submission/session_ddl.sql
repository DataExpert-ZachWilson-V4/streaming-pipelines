/*
Write a DDL query (session_ddl.sql) that creates a table that tracks Data Expert sessions.

Make sure to have a unique identifier for each session
the start and end of the session
a unique identifier for the session
how many events happened in that session
the date of the beginning of the session
What city, country, and state is associated with this session
What operating system and browser are associated with this session
Whether this session is for logged in or logged out users
*/


CREATE OR REPLACE TABLE shruthishridhar.dataexpertsessions (
    session_id VARCHAR PRIMARY KEY, -- Unique identifier for each session
    user_id VARCHAR, -- User identifier if an user is logged in
    start_time TIMESTAMP NOT NULL, -- Start of the session
    end_time TIMESTAMP NOT NULL, -- End of the session
    event_count INTEGER NOT NULL, -- Number of events happened in the session
    start_date DATE NOT NULL, -- Date of the beginning of the session
    city VARCHAR, -- City associated with the session
    state VARCHAR, -- State associated with the session
    country VARCHAR, -- Country associated with the session
    operating_system VARCHAR, -- Operating system associated with the session
    browser VARCHAR, -- Browser associated with the session
    is_logged_in BOOLEAN NOT NULL -- Whether the session is for logged in or logged out users
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['start_date']
)