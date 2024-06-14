CREATE TABLE IF NOT EXISTS {output_table}
(
    session_id STRING,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count BIGINT,
    session_begin_date DATE,
    city STRING,
    country STRING,
    state STRING,
    os STRING,
    browser STRING,
    user_id BIGINT,
    is_logged BOOLEAN
)
WITH
    (
    format = 'PARQUET',
    partitioning = ARRAY['session_begin_date']
    )
