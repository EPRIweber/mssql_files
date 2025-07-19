-- models/staging/stg_courses.sql

WITH ranked_matches AS (
  SELECT
    s.cleaned_name,
    s.source_name,
    s.source_id,
    u.instnm,
    s.src_host,
    u.uni_host,
    ROW_NUMBER() OVER (
      PARTITION BY s.source_name
      ORDER BY
        CASE
          WHEN s.src_host = u.uni_host
            OR s.src_host LIKE '%.' + u.uni_host
          THEN 1    -- prioritize URL match
          ELSE 2    -- fallback to name match
        END
    ) AS rn
  FROM {{ source('dbo', 'sources') }} AS s
  JOIN {{ source('dbo', 'universities') }} AS u
    ON s.src_host    = u.uni_host
    OR s.src_host LIKE '%.' + u.uni_host
    OR LOWER(s.cleaned_name) LIKE '%' + LOWER(u.instnm) + '%'
)
SELECT *
FROM ranked_matches
WHERE rn = 1;