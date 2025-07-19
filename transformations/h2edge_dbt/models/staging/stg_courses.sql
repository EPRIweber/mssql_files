-- models/staging/stg_courses.sql

select
    course_id,
    course_source_id,
    course_title,
    course_description,
    course_code,
    courses_crtd_id,
    courses_crtd_dt,
    courses_updt_id,
    courses_updt_dt,
    courses_table_id

from {{ source('dbo', 'courses') }}