-- V48__altering_cleaned_name.sql

ALTER TABLE sources
DROP COLUMN cleaned_name;
GO

ALTER TABLE sources
  ADD cleaned_name AS (
    REPLACE(
      TRIM('_ ' FROM
        /* 1) strip any trailing " src_<digits>" */
        REPLACE(
          REPLACE(
            CASE
              WHEN CHARINDEX(' src_', LOWER(source_name)) > 0
                THEN LEFT(
                       LOWER(source_name),
                       CHARINDEX(' src_', LOWER(source_name)) - 1
                     )
              ELSE LOWER(source_name)
            END,
            /* 2) remove 'undergraduate' */
            'undergraduate', ''
          ),
          /* 3) remove 'graduate' */
          'graduate', ''
        )
      ),
      /* 4) turn underscores into spaces */
      '_', ' '
    )
  ) PERSISTED;
GO
