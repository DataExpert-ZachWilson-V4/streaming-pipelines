-- Creating the table to store web event details for each user session
-- The table includes columns for session ID, session start and end times, event count, session start date, geographical information, OS and browser details, and login status

CREATE OR REPLACE TABLE shashankkongara.kafka_streaming_user_sessions_homework (
    session_id INT,  -- Unique identifier for each session, hashed from user_id, ip, and session start time
    window_start TIMESTAMP,  -- Start time of the session
    window_end TIMESTAMP,  -- End time of the session
    event_count BIGINT,  -- Total number of events that occurred during the session
    start_date DATE,  -- Date when the session started
    country VARCHAR,  -- Country associated with the session, based on IP geolocation
    state VARCHAR,  -- State associated with the session, based on IP geolocation
    city VARCHAR,  -- City associated with the session, based on IP geolocation
    device_family VARCHAR,  -- Operating system used during the session, logged at the family level (e.g., "Windows", "MacOS")
    browser_family VARCHAR,  -- Browser used during the session, logged at the family level (e.g., "Chrome", "Firefox")
    login_status VARCHAR  -- Status indicating whether the session was for logged-in or logged-out users
)
WITH (
    format = 'PARQUET',  -- Specify the storage format for the table as Parquet
    partitioning = ARRAY['start_date']  -- Partition the table by the start_date column for efficient querying
)
