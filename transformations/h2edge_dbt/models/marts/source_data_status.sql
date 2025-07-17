-- models/marts/source_data_status.sql

{{
  config(
    materialized='view'
  )
}}

WITH schema_counts AS (
  SELECT
    scraper_schema_source_id,
    COUNT(*) AS schema_count
  FROM {{
    ref('stg_scraper_schemas')
  }}
  GROUP BY scraper_schema_source_id
),

url_counts AS (
  SELECT
    url_source_id,
    COUNT(*) AS url_count
  FROM {{
    ref('stg_urls')
  }}
  GROUP BY url_source_id
),

course_counts AS (
  SELECT
    course_source_id,
    COUNT(*) AS course_count
  FROM {{ ref('stg_courses') }}
  GROUP BY course_source_id
)

SELECT
  s.source_name,
  COALESCE(u.url_count, 0)    AS url_count,
  COALESCE(c.course_count, 0) AS course_count
FROM {{ ref('stg_sources') }} AS s
LEFT JOIN schema_counts AS sc
ON sc.source_id = s.source_id
LEFT JOIN url_counts AS u
ON u.url_source_id = s.source_id
LEFT JOIN course_counts AS c
ON c.course_source_id = s.source_id;
