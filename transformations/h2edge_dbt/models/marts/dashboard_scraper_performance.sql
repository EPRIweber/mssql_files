-- models/marts/dashboard_scraper_performance.sql

{{ config(materialized='view') }}

WITH source_scrape_complete AS (
    SELECT 
        s.source_id,
        MAX(l.log_ts) AS scrape_ts,
        MAX(l.log_run_id) AS max_run_id
    FROM {{ ref('stg_sources') }} s
    LEFT JOIN {{ ref('stg_logs') }} l
      ON s.source_id = l.log_source_id
     AND l.log_message LIKE '%records scraped%'
    LEFT JOIN {{ ref('stg_logs') }} l_backup
      ON s.source_id = l_backup.log_source_id
    GROUP BY s.source_id
),
source_scrape_start AS (
  SELECT
    s.source_id,
    MAX(l.log_ts) AS scrape_ts
  FROM {{ ref('stg_sources') }} s
  LEFT JOIN source_scrape_complete ssc
    ON ssc.source_id = s.source_id
  LEFT JOIN {{ ref('stg_logs') }} l
    ON l.log_source_id = ssc.source_id
    AND l.log_message LIKE '%no data found, scraping%'
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
    s.cleaned_name AS canonical_name,
    s.source_name,
    uc.url_count,
    cc.course_count,
    r.run_id,
    r.run_status,
    FORMAT(sss.scrape_ts, 'yyyy-MM-dd HH-mm') AS start_scrape_ts,
    FORMAT(ssc.scrape_ts, 'yyyy-MM-dd HH-mm') AS end_scrape_ts,
    --FORMAT(r.run_start_time, 'yyyy-MM-dd HH:mm') AS run_start_ts,
    --FORMAT(r.run_end_time, 'yyyy-MM-dd HH:mm') AS run_end_ts,
    --ec.extracted_count,
    --FORMAT(ssc.scrape_ts, 'yyyy-MM-dd HH:mm') AS scrape_complete_ts,
    CONVERT(
      VARCHAR(8),
      DATEADD(
        SECOND,
        DATEDIFF(SECOND, sss.scrape_ts, ssc.scrape_ts), 0
      ), 108
    ) AS scrape_time_hh_mm_ss,
    sa.slots_left,
    CASE
        WHEN cc.course_count > 0 THEN CAST(ssc.scrape_ts AS VARCHAR(30))
        ELSE NULL
    END AS courses_extracted_date
FROM source_scrape_complete ssc
JOIN {{ ref('stg_sources') }} s ON s.source_id = ssc.source_id
LEFT JOIN source_scrape_start sss
ON s.source_id = sss.source_id

-- log that contains the "X records scraped" message at the scrape completion
LEFT JOIN {{ ref('stg_logs') }} l
  ON l.log_ts = ssc.scrape_ts
 AND ssc.source_id = l.log_source_id
 AND ssc.max_run_id = l.log_run_id
 AND l.log_message NOT LIKE 'writing records%'

-- extract the number of records scraped
OUTER APPLY (
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
--WHERE r.run_id IS NOT NULL
;