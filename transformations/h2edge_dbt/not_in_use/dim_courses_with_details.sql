-- models/marts/dim_courses_with_details.sql

{{
  config(
    materialized='view'
  )
}}

WITH courses AS (
    SELECT * FROM {{ ref('stg_courses') }}
),

sources AS (
    SELECT * FROM {{ ref('stg_sources') }}
),

course_taxonomy AS (
    SELECT * FROM {{ ref('stg_course_taxonomy') }}
),

taxonomies AS (
    SELECT * FROM {{ ref('stg_taxonomies') }}
),

final_model AS (
    SELECT
        t.taxonomy_id,
        t.taxonomy_description,
        c.course_title,
        c.course_description,
        s.university_name
    FROM
        courses AS c
    JOIN
        sources AS s ON c.source_id = s.source_id
    JOIN
        course_taxonomy AS ct ON c.course_id = ct.course_id
    JOIN
        taxonomies AS t ON ct.taxonomy_id = t.taxonomy_id
)

SELECT * FROM final_model
