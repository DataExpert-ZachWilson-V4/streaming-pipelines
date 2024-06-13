CREATE TABLE IF NOT EXISTS jrsarrat.streaming_hw_week5 (
    session_id STRING COMMENT 'Unique identifier',
    session_start TIMESTAMP COMMENT 'Start time of session',
    session_end TIMESTAMP COMMENT 'End time of session',
    event_count INT COMMENT 'Number of events in the associated session',
    session_date DATE COMMENT 'Beginning of the session',
    city STRING COMMENT 'City associated with session',
    state STRING COMMENT 'State associated with session',
    country STRING COMMENT 'Country associated with session',
    operating_system STRING COMMENT 'Operating system used during session',
    browser STRING COMMENT 'Browser used during session',
    user_status STRING COMMENT 'Logged in or logged out user status'
)
USING iceberg
COMMENT 'tbl tracking DataExpert.io user sessions';
