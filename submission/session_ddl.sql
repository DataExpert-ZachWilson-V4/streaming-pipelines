
-- Write a DDL query (session_ddl.sql) that creates a table that tracks Data Expert sessions.

--     Make sure to have a unique identifier for each session
--     the start and end of the session
--     a unique identifier for the session
--     how many events happened in that session
--     the date of the beginning of the session
--     What city, country, and state is associated with this session
--     What operating system and browser are associated with this session
--     Whether this session is for logged in or logged out user



CREATE
OR REPLACE TABLE hariomnayani88482.streaming_sessions_tracking (
  id varchar,
  country varchar,
  state varchar,
  city varchar,
  operating_system varchar,
  browser varchar,
  start_time timestamp,
  end_time timestamp,
  session_date DATE,
  n_events integer,
  logged_in boolean
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_date']
)