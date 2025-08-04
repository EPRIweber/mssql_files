-- models/staging/stg_sources.sql

SELECT
    source_id,
    source_name,
    is_enabled,
    cleaned_name
FROM
    {{ source('dbo', 'sources') }}
WHERE is_enabled = 1;