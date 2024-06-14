CREATE OR REPLACE TABLE raj.DataExpertSessions 
(
    session_id INTEGER PRIMARY KEY, -- Unique identifier for each session
    session_start TIMESTAMP(6) WITH TIME ZONE  NOT NULL, -- Start of the session
    session_end TIMESTAMP(6) WITH TIME ZONE  NOT NULL, -- End of the session
    event_count INT NOT NULL, -- Number of events happened in the session
    session_date DATE NOT NULL, -- Date of the beginning of the session
    city VARCHAR(100) NOT NULL, -- City associated with the session
    state VARCHAR(100) NOT NULL, -- State associated with the session
    country VARCHAR(100) NOT NULL, -- Country associated with the session
    operating_system VARCHAR(100) NOT NULL, -- Operating system associated with the session
    browser VARCHAR(100) NOT NULL, -- Browser associated with the session
    is_logged_in BOOLEAN NOT NULL -- Whether the session is for logged in or logged out users
)
WITH (
    format = 'PARQUET',  
    partitioning = ARRAY['session_start']  
)
