create or replace table shabab.spark_session (
    session_id          varchar,
    user_id             varchar,

    session_window_start       timestamp,
    session_window_end         timestamp,
    session_date        date,

    city                varchar,
    state               varchar,
    country             varchar,

    os                  varchar,  -- operating system used during session
    browser             varchar,  -- browser used during session

    is_logged_in        boolean,
    host                varchar   -- host associated with session

    event_count         bigint
) with (
    PARTITIONING = Array['session_date']
)
