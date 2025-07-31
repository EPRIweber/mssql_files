-- models/staging/stg_sources.sql

SELECT
    source_id,
    source_name,
    cleaned_name
FROM
    {{ source('dbo', 'sources') }}