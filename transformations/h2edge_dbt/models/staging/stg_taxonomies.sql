-- models/staging/stg_taxonomies.sql

SELECT
    taxonomy_id,
    taxonomy_description
FROM
    {{ source('dbo', 'taxonomy01') }}