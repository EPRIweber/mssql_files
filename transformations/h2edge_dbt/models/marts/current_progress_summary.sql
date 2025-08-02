{{ config(materialized='view') }}

WITH latest_course_scrape AS (
    SELECT
        s.source_distinct_id,
        MAX(l.log_ts) AS last_scrape_ts
    FROM {{ ref('logs') }} l
    JOIN {{ ref('sources') }} s ON s.source_id = l.log_source_id
    WHERE l.log_stage = 2
      AND LOWER(l.log_message) LIKE '%scrape succeeded%'
    GROUP BY s.source_distinct_id
),
aggregated_status AS (
    SELECT
        COALESCE(ds.distinct_id, s.source_distinct_id) AS school_id,
        COALESCE(ds.distinct_name, ss.cleaned_name) AS school_name,
        SUM(COALESCE(sd.schema_count, 0)) AS schema_count,
        SUM(COALESCE(sd.url_count, 0)) AS url_count,
        SUM(COALESCE(sd.course_count, 0)) AS course_count,
        MAX(CAST(l.logs_crtd_dt AS TIMESTAMP)) AS last_scrape_ts,
        MAX(ds.distinct_scraper_status) AS distinct_scraper_status
    FROM {{ ref('source_data_status') }} sd
    JOIN {{ ref('stg_sources') }} ss ON ss.cleaned_name = sd.cleaned_name
    JOIN {{ ref('sources') }} s ON s.source_id = ss.source_id
    LEFT JOIN {{ ref('distinct_sources') }} ds ON s.source_distinct_id = ds.distinct_id
    LEFT JOIN latest_course_scrape lcs ON lcs.source_distinct_id = s.source_distinct_id
    LEFT JOIN {{ ref('logs') }} l ON l.log_source_id = s.source_id
    GROUP BY COALESCE(ds.distinct_id, s.source_distinct_id), COALESCE(ds.distinct_name, ss.cleaned_name)
),
latest_log AS (
    SELECT
        COALESCE(ds.distinct_id, s.source_distinct_id) AS school_id,
        l.log_message,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(ds.distinct_id, s.source_distinct_id)
            ORDER BY l.log_ts DESC
        ) AS rn
    FROM {{ ref('logs') }} l
    JOIN {{ ref('sources') }} s ON s.source_id = l.log_source_id
    LEFT JOIN {{ ref('distinct_sources') }} ds ON s.source_distinct_id = ds.distinct_id
),
latest_log_per_school AS (
    SELECT
        school_id,
        log_message AS latest_log_message
    FROM latest_log
    WHERE rn = 1
)
SELECT
    a.school_id,
    a.school_name,
    a.schema_count,
    a.url_count,
    a.course_count,
    CASE WHEN a.course_count > 0 THEN 1 ELSE 0 END AS has_courses,
    CAST(a.last_scrape_ts AS STRING) AS last_scrape_human,
    CASE
        WHEN a.course_count > 0 THEN 'Data Present'
        WHEN a.last_scrape_ts IS NOT NULL THEN 'Scraped, No Courses'
        WHEN a.url_count > 0 THEN 'Crawled, Pending Scrape'
        WHEN a.schema_count > 0 THEN 'Schema Generated'
        ELSE 'Pending'
    END AS summary_status,
    COALESCE(
        NULLIF(a.distinct_scraper_status, ''),
        ll.latest_log_message,
        CASE
            WHEN a.course_count > 0 THEN 'Data Present'
            WHEN a.last_scrape_ts IS NOT NULL THEN 'Scraped, No Courses'
            WHEN a.url_count > 0 THEN 'Crawled, Pending Scrape'
            WHEN a.schema_count > 0 THEN 'Schema Generated'
            ELSE 'Pending'
        END
    ) AS status_indicator,
    a.last_scrape_ts
FROM aggregated_status a
LEFT JOIN latest_log_per_school ll ON ll.school_id = a.school_id
-- Uncomment the line below to filter out null scrape dates
-- WHERE a.last_scrape_ts IS NOT NULL
;
