CREATE OR REPLACE TABLE saismail.session_data (
    "url" VARCHAR, 
    referrer VARCHAR, 
    user_agent ROW(
    family VARCHAR, 
    major VARCHAR, 
    minor VARCHAR, 
    patch VARCHAR, 
    device ROW(
        family VARCHAR, 
        major VARCHAR, 
        minor VARCHAR, 
        patch VARCHAR
    ), 
    os ROW(
        family VARCHAR, 
        major VARCHAR, 
        minor VARCHAR, 
        patch VARCHAR
    )
    ), 
    headers map(VARCHAR, VARCHAR), 
    host VARCHAR, 
    ip VARCHAR, 
    event_time TIMESTAMP(6) with time zone
)
WITH (
  format = 'PARQUET'
)
