create or replace table dennisgera.dataexpertsessions (
    session_id VARCHAR, -- Unique identifier 
    user_id VARCHAR, -- User identifier, if logged in
    started_at TIMESTAMP, -- Timestamp of the start of the session
    ended_at TIMESTAMP, -- Timestamp of the end of the session
    event_count INTEGER, -- Count of events in the session
    start_date DATE, -- Date of the start of the session
    city VARCHAR, -- City where the session originated
    state VARCHAR, -- State where the session originated
    country VARCHAR, -- Country where the session originated
    operating_system VARCHAR, -- Operating system associated with the session
    browser VARCHAR, -- Browser associated with the session
    is_logged_in BOOLEAN -- Whether the session is for logged in or logged out users
)
with (
    partitioning = array['start_date']
)