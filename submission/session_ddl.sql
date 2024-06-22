CREATE TABLE IF NOT EXISTS  martinaandrulli.data_expert_sessions (
    session_id VARCHAR,
    session_start TIMESTAMP,
    session_end TIMESTAMP,                  
    event_count BIGINT,          
    session_start_date DATE,            
    city VARCHAR,        
    state VARCHAR,                    
    country VARCHAR,                  
    os VARCHAR,
    browser VARCHAR,
    user_id VARCHAR,             
    is_logged_in BOOLEAN
)
WITH (
    format = 'PARQUET',  
    partitioning = ARRAY['session_start_date']  
)