CREATE TABLE DATA_EXPERTS_SESSIONS (
    session_id BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_date DATE,
    event_count INTEGER,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    device_family VARCHAR(100),
    browser_family VARCHAR(100),
    is_logged_in BOOLEAN,
    user_id VARCHAR(255)
)
USING ICEBERG
PARTITIONED BY (session_date)