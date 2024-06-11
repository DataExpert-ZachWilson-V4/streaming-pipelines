CREATE TABLE IF NOT EXISTS {output_table} (
    session_id STRING NOT NULL,
    user_id STRING,
    ip STRING,
    city STRING,
    state STRING,
    country STRING,
    os STRING,
    browser STRING,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_date DATE,
    event_count BIGINT,
    is_logged BOOLEAN
)
USING ICEBERG
PARTITIONED BY (session_date);