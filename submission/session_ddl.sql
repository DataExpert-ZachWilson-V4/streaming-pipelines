
CREATE TABLE IF NOT EXISTS {output_table} (
  id BIGINT,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  session_id STRING,
  number_of_events BIGINT,
  session_begin_date DATE,
  city STRING,
  state STRING,
  country STRING,
  operating_system STRING,
  browser STRING,
  is_logged BOOLEAN
  )
  WITH
  (
    format = 'PARQUET',
    partitioning = ARRAY['session_begin_date']
    )
  
