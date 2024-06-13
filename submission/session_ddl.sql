CREATE
OR REPLACE TABLE mariavyso.dataexpert_sessions (
    session_id STRING,
    user_id STRING,
    start_session TIMESTAMP,
    end_session TIMESTAMP,
    start_session_date DATE,
    event_count BIGINT,
    country STRING,
    state STRING,
    city STRING,
    os STRING,
    browser STRING,
    is_logged BOOLEAN
)
WITH
    (
        format = 'PARQUET',
        partitioning = ARRAY['session_start_date']
    )
