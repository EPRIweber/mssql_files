-- models/marts/current_progress_summary.sql

{{ config(materialized='view') }}

WITH source_scrape_complete AS (
    SELECT 
        source_id,
        MAX(log_ts) AS scrape_ts,
        MAX(log_run_id) AS max_run_id
    FROM {{ ref('stg_sources') }} s
    LEFT JOIN {{ ref('stg_logs') }} l
    ON (
        s.source_id = l.log_source_id
        AND l.log_message LIKE '%records scraped%'
    )
    GROUP BY s.source_id
),
latest_log_per_school AS (
    SELECT
        ss.cleaned_name AS school_name,
        l.log_message AS latest_log_message
    FROM (
        SELECT
            ss.cleaned_name,
            l.log_message,
            ROW_NUMBER() OVER (
                PARTITION BY ss.cleaned_name
                ORDER BY l.log_ts DESC
            ) AS rn
        FROM {{ ref('stg_logs') }} l
        JOIN {{ ref('stg_sources') }} ss ON ss.source_id = l.log_source_id
    ) l
    WHERE rn = 1
)

SELECT
    sd.cleaned_name AS school_name,
    sd.schema_count,
    sd.url_count,
    sd.course_count,
    CASE WHEN sd.course_count > 0 THEN 1 ELSE 0 END AS has_courses,
    CAST(lcs.last_scrape_ts AS STRING) AS last_scrape_human,
    CASE
        WHEN sd.course_count > 0 THEN 'Data Present'
        WHEN lcs.last_scrape_ts IS NOT NULL THEN 'Scraped, No Courses'
        WHEN sd.url_count > 0 THEN 'Crawled, Pending Scrape'
        WHEN sd.schema_count > 0 THEN 'Schema Generated'
        ELSE 'Pending'
    END AS summary_status,
    COALESCE(
        llps.latest_log_message,
        CASE
            WHEN sd.course_count > 0 THEN 'Data Present'
            WHEN lcs.last_scrape_ts IS NOT NULL THEN 'Scraped, No Courses'
            WHEN sd.url_count > 0 THEN 'Crawled, Pending Scrape'
            WHEN sd.schema_count > 0 THEN 'Schema Generated'
            ELSE 'Pending'
        END
    ) AS status_indicator,
    lcs.last_scrape_ts
FROM {{ ref('source_data_status') }} sd
LEFT JOIN latest_course_scrape lcs
    ON lcs.school_name = sd.cleaned_name
LEFT JOIN latest_log_per_school llps
    ON llps.school_name = sd.cleaned_name
-- WHERE lcs.last_scrape_ts IS NOT NULL
;
