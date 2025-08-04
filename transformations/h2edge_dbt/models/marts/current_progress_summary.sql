-- models/marts/current_progress_summary.sql

{{ config(materialized='view') }}

WITH latest_course_scrape AS (
    SELECT
        school_name,
        last_scrape_ts,
        scraped_log_message
    FROM (
        SELECT
            ss.cleaned_name AS school_name,
            l.log_message AS scraped_log_message,
            l.log_ts AS last_scrape_ts,
            ROW_NUMBER() OVER (
                PARTITION BY ss.cleaned_name
                ORDER BY l.log_ts DESC
            ) AS rn
        FROM {{ ref('stg_logs') }} l
        JOIN {{ ref('stg_sources') }} ss ON ss.source_id = l.log_source_id
        WHERE LOWER(l.log_message) LIKE '%records scraped%'
    ) t
    WHERE rn = 1
),
latest_log_per_school AS (
    SELECT
        school_name,
        latest_log_message,
        log_run_id
    FROM (
        SELECT
            ss.cleaned_name AS school_name,
            l.log_message AS latest_log_message,
            l.log_run_id,
            ROW_NUMBER() OVER (
                PARTITION BY ss.cleaned_name
                ORDER BY l.log_ts DESC
            ) AS rn
        FROM {{ ref('stg_logs') }} l
        JOIN {{ ref('stg_sources') }} ss ON ss.source_id = l.log_source_id
    ) t
    WHERE rn = 1
)

SELECT
    sd.cleaned_name AS school_name,
    sd.schema_count,
    sd.url_count,
    sd.course_count,
    CASE WHEN sd.course_count > 0 THEN 1 ELSE 0 END AS has_courses,
    CASE
        WHEN lcs.last_scrape_ts IS NOT NULL THEN FORMAT(lcs.last_scrape_ts, 'yyyy-MM-dd HH:mm')
        ELSE '0'
    END AS last_scrape_ts,
    CASE
        WHEN sd.course_count > 0 THEN 'Data Present'
        WHEN lcs.last_scrape_ts IS NOT NULL THEN 'Scraped, No Courses'
        WHEN sd.url_count > 0 THEN 'Crawled, Pending Scrape'
        WHEN sd.schema_count > 0 THEN 'Schema Generated'
        ELSE 'Pending'
    END AS summary_status,
    llps.latest_log_message AS last_log,
    llps.log_run_id AS run_id,
    r.run_status AS run_status
    /*
    COALESCE(
        llps.latest_log_message,
        CASE
            WHEN sd.course_count > 0 THEN 'Data Present'
            WHEN lcs.last_scrape_ts IS NOT NULL THEN 'Scraped, No Courses'
            WHEN sd.url_count > 0 THEN 'Crawled, Pending Scrape'
            WHEN sd.schema_count > 0 THEN 'Schema Generated'
            ELSE 'Pending'
        END
    ) AS status_indicator
    */
FROM {{ ref('source_data_status') }} sd
LEFT JOIN latest_course_scrape lcs
    ON lcs.school_name = sd.cleaned_name
LEFT JOIN latest_log_per_school llps
    ON llps.school_name = sd.cleaned_name
LEFT JOIN {{ ref('stg_runs') }} r
    ON run_id = llps.log_run_id
;