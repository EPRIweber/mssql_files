-- models/staging/stg_runs.sql

SELECT
  run_id,
  run_status,
  run_start_time,
  run_end_time,
  runs_crtd_id,
  runs_crtd_dt,
  runs_updt_id,
  runs_updt_dt,
  runs_table_id
FROM {{ source('dbo', 'runs') }}
