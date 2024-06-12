CREATE
OR REPLACE barrocaeric.session_stream (
    session_id BIGINT,
    user_id VARCHAR,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count BIGINT,
    country VARCHAR,
    state VARCHAR,
    city VARCHAR,
    os_type VARCHAR,
    browser_type VARCHAR,
    is_loggedin BOOLEAN
)