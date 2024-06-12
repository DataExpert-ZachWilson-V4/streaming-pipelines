CREATE TABLE IF NOT EXISTS sarneski44638.spark_sessionization (
    session_id BIGINT,
    user_id BIGINT,
    ip STRING,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count BIGINT,
    session_date DATE,
    city STRING,
    state STRING,
    country STRING,
    os STRING,
    browser STRING,
    is_logged_in BOOLEAN
) USING ICEBERG
PARTITION BY
    session_date;