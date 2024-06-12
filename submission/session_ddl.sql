-- TrinoSQL DDL for the streaming_sessions_tracking table
CREATE OR REPLACE TABLE jsgomez14.streaming_sessions_tracking (
    id VARCHAR,
    country VARCHAR,
    state VARCHAR,
    city VARCHAR,
    operating_system VARCHAR,
    browser VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    n_events INTEGER,
    logged_in BOOLEAN
);

-- SparkSQL for the streaming_sessions_tracking table
CREATE TABLE IF NOT EXISTS jsgomez14.streaming_sessions_tracking (
    id STRING,
    country STRING,
    state STRING,
    city STRING,
    operating_system STRING,
    browser STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    n_events INTEGER,
    logged_in BOOLEAN
)
USING ICEBERG;