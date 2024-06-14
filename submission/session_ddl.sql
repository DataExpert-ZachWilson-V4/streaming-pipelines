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

CREATE OR REPLACE TABLE danieldavid.dataexpert_sessions (
    session_id VARCHAR PRIMARY KEY, -- 2. Primary key for the session
    user_id VARCHAR, -- Foreign key of the user
    session_start TIMESTAMP, -- 1. Start of the session
    session_end TIMESTAMP, -- 1. End of the session
    event_count INTEGER, -- 3. Count of events in the session
    session_date DATE, -- 4. Date of the beginning of the session
    city VARCHAR, -- 5. City associated with the session
    state VARCHAR, -- 5. State associated with the session
    country VARCHAR, -- 5. Country associated with the session
    operating_system VARCHAR, -- 6. Operating system associated with the session
    browser VARCHAR, -- 6. Browser associated with the session
    status_logged_in BOOLEAN -- 7. Boolean if user is logged in
)
USING ICEBERG