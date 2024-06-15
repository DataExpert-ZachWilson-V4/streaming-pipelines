create or replace table dennisgera.dataexpertsessions (
    session_id VARCHAR, -- Unique identifier 
    user_id VARCHAR, -- User identifier, if logged in
    window_start TIMESTAMP, -- Timestamp of the start of the session
    window_end TIMESTAMP, -- Timestamp of the end of the session
    event_count INTEGER, -- Count of events in the session
    session_date DATE, -- Date of the start of the session
    city VARCHAR, -- City where the session originated
    state VARCHAR, -- State where the session originated
    country VARCHAR, -- Country where the session originated
    device_family VARCHAR, -- Operating system associated with the session
    browser_family VARCHAR, -- Browser associated with the session
    is_logged_in BOOLEAN -- Whether the session is for logged in or logged out users
)
with (
    partitioning = array['session_date']
)