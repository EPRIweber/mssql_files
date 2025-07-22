-- models/marts/rpt_university_enrollment.sql
-- This model aggregates university enrollment data by type, state, and year
-- to power the final mini-report.

-- CTE for staged university data, providing clean categories
WITH universities AS (
    SELECT
        unitid,
        university_type,
        stabbr
    FROM
        {{ ref('stg_universities') }}
    WHERE
        -- We only want to report on categorized 4-year and 2-year institutions
        university_type IN ('Four-year', 'Two-year')
),

-- CTE for staged enrollment data, providing clean column names
enrollment AS (
    SELECT
        unitid,
        report_year,
        headcount
    FROM
        {{ ref('stg_enrollment_fact') }}
    WHERE
        -- Filter for 'All students total' (EFFYALEV code 1) to get the primary metric
        -- and avoid double-counting with other levels like 'Full-time' or 'Part-time'.
        enrollment_level_code = 1
),

-- CTE to dynamically find the most recent year of reporting in the dataset
most_recent_year AS (
    SELECT MAX(report_year) AS max_year FROM enrollment
)

-- Final SELECT statement to join, aggregate, and build the report table
SELECT
    u.university_type,
    u.stabbr AS state_abbreviation,
    e.report_year,
    SUM(e.headcount) AS total_enrollment,
    -- This flag makes it easy to filter for the latest data in a BI tool or query
    CASE
        WHEN e.report_year = (SELECT max_year FROM most_recent_year) THEN 'Yes'
        ELSE 'No'
    END AS is_most_recent_year
FROM
    enrollment e
JOIN
    universities u ON e.unitid = u.unitid
GROUP BY
    u.university_type,
    u.stabbr,
    e.report_year
-- The ORDER BY clause has been removed from here.