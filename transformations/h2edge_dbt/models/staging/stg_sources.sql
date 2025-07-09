-- models/staging/stg_sources.sql

SELECT
    source_id,
    source_name AS university_name
FROM
    {{ source('dbo', 'sources') }}