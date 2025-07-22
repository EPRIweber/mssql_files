-- models/staging/stg_universities.sql

SELECT
    unitid,
    instnm,
    city,
    stabbr,
    control,
    -- Categorize universities based on the Carnegie 'c21szset' classification
    CASE
        WHEN c21szset BETWEEN 1 AND 5 THEN 'Two-year'
        WHEN c21szset BETWEEN 6 AND 17 THEN 'Four-year'
        ELSE 'Other/Unclassified'
    END AS university_type
FROM
    {{ source('dbo', 'universities') }}