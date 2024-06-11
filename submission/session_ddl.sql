CREATE OR REPLACE TABLE mposada.dataexpert_sessions (
    session_id VARCHAR, -- Unique identifier for the session
    user_id VARCHAR, -- Unique identifier for the user
    session_start TIMESTAMP, -- Start time of the session
    session_end TIMESTAMP, -- End time of the session
    event_count INTEGER, -- Number of events in the session
    session_date DATE, -- Date of the session
    city VARCHAR, -- City from which the session originated
    state VARCHAR, -- State from which the session originated
    country VARCHAR, -- Country from which the session originated
    operating_system VARCHAR, -- Operating system used during the session
    browser VARCHAR, -- Browser used during the session
    is_logged_in BOOLEAN, -- Whether the user was logged in
    PRIMARY KEY (session_id)
)
PARTITION BY session_date
