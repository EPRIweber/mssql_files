ALTER TABLE distinct_sources
ADD search_query NVARCHAR(500);


ALTER TABLE dbo.universities
ADD university_distinct_id UNIQUEIDENTIFIER NULL
    CONSTRAINT FK_universities_distinct
    FOREIGN KEY (university_distinct_id) REFERENCES dbo.distinct_sources(distinct_id);
GO






WITH ranked_matches AS (
  SELECT
    s.source_id,
    s.source_distinct_id,
    u.unitid,
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
  FROM sources AS s
  LEFT JOIN universities AS u
    ON s.src_host = u.uni_host
    OR s.src_host LIKE '%.' + u.uni_host
    OR LOWER(s.cleaned_name) LIKE '%' + LOWER(u.instnm) + '%'
    OR lower(u.instnm) LIKE '%' + LOWER(s.cleaned_name) + '%'
),

cleaned AS (
SELECT DISTINCT
    source_id,
    source_distinct_id,
    unitid
FROM ranked_matches
WHERE rn = 1
)


UPDATE u
SET u.university_distinct_id = s.source_distinct_id
FROM dbo.universities AS u
INNER JOIN cleaned AS c
ON u.unitid = c.unitid
INNER JOIN dbo.sources AS s
ON s.source_id = c.source_id;
GO








CREATE OR ALTER TRIGGER dbo.trg_sources_after_insert
ON dbo.sources
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    ------------------------------------------------------------------------
    -- (1) Insert any brand-new distinct_names from inserted.cleaned_name
    ------------------------------------------------------------------------
    INSERT INTO dbo.distinct_sources (distinct_name)
    SELECT DISTINCT i.cleaned_name
    FROM inserted AS i
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.distinct_sources ds
        WHERE ds.distinct_name = i.cleaned_name
    );

    ------------------------------------------------------------------------
    -- (2) Link each new source row to its distinct_sources row
    ------------------------------------------------------------------------
    UPDATE s
    SET s.source_distinct_id = ds.distinct_id
    FROM dbo.sources AS s
    INNER JOIN inserted AS i
        ON s.source_id = i.source_id
    INNER JOIN dbo.distinct_sources AS ds
        ON ds.distinct_name = i.cleaned_name;

    ------------------------------------------------------------------------
    -- (3) EXACT ranked match logic from stg_join_data_sources.sql,
    --     restricted to the INSERTED rows, to pick a single university.
    ------------------------------------------------------------------------
    ;WITH ranked_matches AS (
        SELECT
            s.cleaned_name,
            s.source_name,
            s.source_id,
            u.instnm,
            s.src_host,
            u.uni_host,
            u.unitid,
            u.universities_table_id,
            CASE
                WHEN s.src_host = u.uni_host
                  OR s.src_host LIKE '%.' + u.uni_host
                    THEN 'host_match'
                WHEN LOWER(s.cleaned_name) LIKE '%' + LOWER(u.instnm) + '%'
                  OR LOWER(u.instnm) LIKE '%' + LOWER(s.cleaned_name) + '%'
                    THEN 'name_match'
                ELSE
                    'no_match'
            END AS match_type,
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
        FROM dbo.sources AS s
        INNER JOIN inserted AS i
            ON s.source_id = i.source_id
        LEFT JOIN dbo.universities AS u
            ON s.src_host = u.uni_host
            OR s.src_host LIKE '%.' + u.uni_host
            OR LOWER(s.cleaned_name) LIKE '%' + LOWER(u.instnm) + '%'
            OR LOWER(u.instnm) LIKE '%' + LOWER(s.cleaned_name) + '%'
    ),
    cleaned AS (
        SELECT source_id, unitid
        FROM ranked_matches
        WHERE rn = 1
    )
    -- Update the matched university with the source's distinct_id
    UPDATE u
    SET u.university_distinct_id = s.source_distinct_id
    FROM dbo.universities AS u
    INNER JOIN cleaned AS c
        ON u.unitid = c.unitid
    INNER JOIN dbo.sources AS s
        ON s.source_id = c.source_id;
END;
GO



CREATE TABLE search_results (
	search_results_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	search_results_distinct_id UNIQUEIDENTIFIER FOREIGN KEY
	REFERENCES distinct_sources(distinct_id),
	url NVARCHAR(500) NOT NULL,
	info NVARCHAR(MAX) DEFAULT NULL
);
GO


CREATE UNIQUE INDEX UX_search_results_distinct_url
ON dbo.search_results(search_results_distinct_id, url);