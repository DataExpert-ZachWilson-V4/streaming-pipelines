-- Table to store user session data
CREATE OR REPLACE TABLE ALEEMRAHIL.SESSIONS_LOG(
    session_id VARCHAR, -- Unique identifier for each session
    user_id VARCHAR, -- Unique identifier for the user
    session_start TIMESTAMP, -- Start of the session
    session_end TIMESTAMP, -- End of the session
    event_count INTEGER, -- Number of events in the session
    session_date DATE, -- Date of the beginning of the session
    city VARCHAR, -- City associated with the session
    country VARCHAR, -- Country associated with the session
    state VARCHAR, -- State associated with the session
    operating_system VARCHAR, -- Operating system associated with the session
    browser VARCHAR, -- Browser associated with the session
    is_logged_in BOOLEAN, -- Whether the session is for logged in or logged out users
    PRIMARY KEY (session_id) -- Primary key constraint
)
PARTITION BY DATE(session_id);
