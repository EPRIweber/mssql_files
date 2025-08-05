-- models/marts/source_data_status.sql

{{
  config(
    materialized='view'
  )
}}

WITH schema_counts AS (
  SELECT
    ss.cleaned_name,
    COUNT(*) AS schema_count
  FROM {{
    ref('stg_scraper_schemas')
  }} ssc
  JOIN {{
    ref('stg_sources')
  }} ss
    ON ssc.scraper_schema_source_id = ss.source_id
  GROUP BY ss.cleaned_name
),
url_counts AS (
  SELECT
    ss.cleaned_name,
    COUNT(*) AS url_count
  FROM {{
    ref('stg_urls')
  }} u
  JOIN {{
    ref('stg_sources')
  }} ss
    ON u.url_source_id = ss.source_id
  GROUP BY ss.cleaned_name
),
deduped_courses AS (
  SELECT DISTINCT
    ss.cleaned_name,
    c.course_description,
    c.course_title
  FROM {{
    ref('stg_courses')
  }} c
  JOIN {{
    ref('stg_sources')
  }} ss
    ON c.course_source_id = ss.source_id
),
course_counts AS (
  SELECT
    cleaned_name,
    COUNT(*) AS course_count
  FROM deduped_courses
  GROUP BY cleaned_name
),
source_count AS (
  SELECT
    cleaned_name,
    COUNT(*) AS source_count
  FROM {{ ref('stg_sources') }}
  GROUP BY cleaned_name
)


SELECT
  s.cleaned_name,
  COALESCE(ssc.source_count, 0) AS source_count,
  COALESCE(sc.schema_count, 0) AS schema_count,
  COALESCE(u.url_count, 0) AS url_count,
  COALESCE(c.course_count, 0) AS course_count
FROM (
  SELECT DISTINCT cleaned_name
  FROM {{
    ref('stg_sources_enabled')
  }}
) AS s
LEFT JOIN source_count ssc ON ssc.cleaned_name = s.cleaned_name
LEFT JOIN schema_counts sc ON sc.cleaned_name = s.cleaned_name
LEFT JOIN url_counts u ON u.cleaned_name = s.cleaned_name
LEFT JOIN course_counts c ON c.cleaned_name = s.cleaned_name;