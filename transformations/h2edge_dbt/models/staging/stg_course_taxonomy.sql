-- models/staging/stg_course_taxonomy.sql

SELECT
    course_id,
    taxonomy_id
FROM
    {{ source('dbo', 'course_taxonomy01') }}