-- models/staging/stg_enrollment_fact.sql

SELECT
    -- Primary key for the fact record
    enrollment_id,

    -- Foreign key to join with universities
    unitid,

    -- The year the data was reported
    report_year,

    -- Renaming for clarity; this is the EFFYALEV code
    student_level AS enrollment_level_code,

    -- Renaming for clarity; this is the unduplicated headcount
    total_headcount AS headcount
FROM
    {{ source('dbo', 'enrollment_fact') }}