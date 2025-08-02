-- models/marts/scraper_performance.sql

{{ config(materialized='view') }}

WITH source_scrape_complete AS (
    SELECT 
        s.source_id,
        MAX(l.log_ts) AS scrape_ts,
        MAX(l.log_run_id) AS max_run_id
    FROM {{ ref('stg_sources') }} s
    JOIN {{ ref('stg_logs') }} l
      ON s.source_id = l.log_source_id
     AND l.log_message LIKE '%records scraped%'
    GROUP BY s.source_id
),
url_counts AS (
    SELECT
        url_source_id,
        COUNT(*) AS url_count
    FROM {{ ref('stg_urls') }}
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
    s.cleaned_name,
    s.source_name,
    r.run_id,
    r.run_start_time,
    r.run_end_time,
    ec.extracted_count,
    uc.url_count,
    cc.course_count,
    seml.log_ts AS start_scrape_ts,
    sa.slots_left,
    CAST(ssc.scrape_ts AS VARCHAR(30)) AS courses_extracted_ts
FROM source_scrape_complete ssc
JOIN {{ ref('stg_sources') }} s ON s.source_id = ssc.source_id

-- log that contains the "X records scraped" message at the scrape completion
LEFT JOIN {{ ref('stg_logs') }} l
  ON l.log_ts = ssc.scrape_ts
 AND ssc.source_id = l.log_source_id
 AND ssc.max_run_id = l.log_run_id
 AND l.log_message NOT LIKE 'writing records%'

-- extract the number of records scraped
CROSS APPLY (
    SELECT 
        TRY_CAST(
            LEFT(
                l.log_message,
                NULLIF(CHARINDEX(' records scraped', LOWER(l.log_message)), 0) - 1
            ) AS INT
        ) AS extracted_count
) AS ec

-- log that has the slots-left info
LEFT JOIN {{ ref('stg_logs') }} seml
  ON seml.log_run_id = l.log_run_id
 AND seml.log_message LIKE '%scrape (slots left:%'
 AND seml.log_source_id = ssc.source_id

-- extract slots_left
OUTER APPLY (
    SELECT
        TRY_CAST(
            LTRIM(RTRIM(
                SUBSTRING(
                    seml.log_message,
                    CHARINDEX('(slots left:', LOWER(seml.log_message)) + LEN('(slots left:'),
                    CASE 
                        WHEN CHARINDEX('(slots left:', LOWER(seml.log_message)) > 0 
                             AND CHARINDEX(')', seml.log_message, CHARINDEX('(slots left:', LOWER(seml.log_message))) 
                                 > CHARINDEX('(slots left:', LOWER(seml.log_message)) + LEN('(slots left:')
                        THEN CHARINDEX(')', seml.log_message, CHARINDEX('(slots left:', LOWER(seml.log_message)))
                             - (CHARINDEX('(slots left:', LOWER(seml.log_message)) + LEN('(slots left:'))
                        ELSE 0
                    END
                )
            )) AS INT
        ) AS slots_left
) AS sa

LEFT JOIN {{ ref('stg_runs') }} r ON r.run_id = ssc.max_run_id
LEFT JOIN url_counts uc ON uc.url_source_id = s.source_id
LEFT JOIN course_counts cc ON cc.course_source_id = s.source_id
WHERE r.run_id IS NOT NULL
;
