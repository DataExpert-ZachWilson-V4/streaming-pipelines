/*
Make sure to have a unique identifier for each session
1. the start and end of the session
2. a unique identifier for the session
3. how many events happened in that session
4. the date of the beginning of the session
5. What city, country, and state is associated with this session
6. What operating system and browser are associated with this session
7. Whether this session is for logged in or logged out users
*/

CREATE OR REPLACE TABLE mymah592.dataexpert_sessions (
    session_id VARCHAR, -- session unique identifier
    user_id VARCHAR, -- ID if logged in, null otherwise
    session_start_dt DATE, -- day of session start
    session_start_ts TIMESTAMP, -- session start
    session_end_ts TIMESTAMP, -- session end
    event_count BIGINT, -- # of events in session
    country VARCHAR, 
    state VARCHAR,
    city VARCHAR,
    os VARCHAR, -- operating system of user
    browser VARCHAR, -- browser of user
    is_logged BOOLEAN -- TRUE = logged in
)
WITH 
(
    format = 'PARQUET',
    partitioning = ARRAY['session_start_date']
)
