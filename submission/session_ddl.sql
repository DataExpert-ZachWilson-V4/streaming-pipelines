create or replace table sanchit.streaming_sessions_tracking (
    id varchar,
    country varchar,
    state varchar,
    city varchar,
    operating_system varchar,
    browser varchar,
    start_time timestamp,
    end_time timestamp,
    n_events integer,
    logged_in boolean
)
