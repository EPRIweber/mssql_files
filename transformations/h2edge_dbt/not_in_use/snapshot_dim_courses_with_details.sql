{% snapshot snapshot_dim_courses_with_details %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='dbt_unique_key',
      check_cols=['taxonomy_description', 'course_description', 'university_name']
    )
}}

with source_data as (
    -- This CTE simply points to your view
    select * from {{ ref('dim_courses_with_details') }}
),

prepared_data as (
    -- This CTE does all the work: it creates the unique key and
    -- explicitly CASTs every single column to its final data type.
    select
        CAST(CONCAT(taxonomy_id, '-', course_title) AS NVARCHAR(350)) as dbt_unique_key,
        CAST(taxonomy_id AS NVARCHAR(50)) AS taxonomy_id,
        CAST(taxonomy_description AS NVARCHAR(MAX)) AS taxonomy_description,
        CAST(course_title AS NVARCHAR(250)) AS course_title,
        CAST(course_description AS NVARCHAR(MAX)) AS course_description,
        CAST(university_name AS NVARCHAR(50)) AS university_name
    from source_data
)

-- The final select is now a simple, unambiguous query from the prepared data.
select * from prepared_data

{% endsnapshot %}
