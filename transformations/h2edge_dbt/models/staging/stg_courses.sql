-- models/staging/stg_courses.sql

select
    course_id,
    course_source_id            as source_id,
    course_title,
    course_description,
    course_code,
    courses_crtd_id             as created_by_id,
    courses_crtd_dt             as created_at,
    courses_updt_id             as updated_by_id,
    courses_updt_dt             as updated_at,
    courses_table_id            as course_unique_id

from {{ source('dbo', 'courses') }}