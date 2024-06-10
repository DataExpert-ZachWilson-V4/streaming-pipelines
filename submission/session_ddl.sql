CREATE OR REPLACE TABLE mposada.dataexpert_sessions (
    session_id VARCHAR,
    user_id VARCHAR,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    event_count INTEGER,
    session_date DATE,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    operating_system VARCHAR,
    browser VARCHAR,
    is_logged_in BOOLEAN,
    PRIMARY KEY (session_id)
)
