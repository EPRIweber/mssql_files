-- models/staging/stg_logs.sql

SELECT
    log_id                  AS log_id,
    log_run_id              AS log_run_id,
    log_source_id           AS log_source_id,
    log_stage               AS log_stage,
    log_message             AS log_message,
    log_ts                  AS log_ts,          -- TIMESTAMP
    logs_crtd_dt            AS logs_crtd_dt     -- If you have this column for created datetime
FROM {{ source('dbo', 'logs') }}
-- Add filters or casting if necessary
