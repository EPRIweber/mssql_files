CREATE OR ALTER PROCEDURE dbo.get_enabled_sources
AS
SELECT
    source_name         AS name,
    source_type         AS type,
    source_base_url     AS root_url,
    source_schema_url   AS schema_url,
    include_external,
    source_crawl_depth  AS crawl_depth,
    page_timeout_s,
    max_concurrency
FROM dbo.sources
WHERE is_enabled = 1;
GO



CREATE OR ALTER PROCEDURE upsert_source
(
    @name NVARCHAR (50),
    @type NVARCHAR (15),
    @base_url NVARCHAR (200) = NULL,
    @schema_url NVARCHAR (200) = NULL,
    @crawl_depth INT = NULL,
    @include_ext BIT = NULL,
    @page_timeout INT = NULL,
    @max_conc INT = NULL
)
AS
/*
-- Used for fetching or inserting source for scraping job

-- Required inputs: name, type
-- Optional inputs: base_url, schema_url, crawl_depth

-- Outputs: source_id of scraping source
*/
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.sources AS tgt
    USING (SELECT
               @name         AS name,
               @type         AS type,
               @base_url     AS base_url,
               @schema_url   AS schema_url,
               @crawl_depth  AS crawl_depth,
               @include_ext  AS include_ext,
               @page_timeout AS page_timeout,
               @max_conc     AS max_conc) AS src
      ON tgt.source_name = src.name
    WHEN MATCHED THEN
        UPDATE SET
            tgt.source_type         = src.type,
            tgt.source_base_url     = src.base_url,
            tgt.source_schema_url   = src.schema_url,
            tgt.source_crawl_depth  = COALESCE(src.crawl_depth , tgt.source_crawl_depth),
            tgt.include_external    = COALESCE(src.include_ext  , tgt.include_external),
            tgt.page_timeout_s      = COALESCE(src.page_timeout , tgt.page_timeout_s),
            tgt.max_concurrency     = COALESCE(src.max_conc     , tgt.max_concurrency)
    WHEN NOT MATCHED THEN
        INSERT (source_name, source_type, source_base_url, source_schema_url,
                source_crawl_depth, include_external, page_timeout_s, max_concurrency)
        VALUES (src.name, src.type, src.base_url, src.schema_url,
                src.crawl_depth, src.include_ext, src.page_timeout, src.max_conc);

    SELECT source_id FROM dbo.sources WHERE source_name = @name;
END;
GO



CREATE OR ALTER PROCEDURE get_target_urls
(
	@source_id_in UNIQUEIDENTIFIER
)
AS
/*
Selects all target urls for given source_id
*/
BEGIN
    SET NOCOUNT ON

	SELECT u.url_link
    FROM urls u
    JOIN sources s ON s.source_id = u.url_source_id
    WHERE s.source_id = @source_id_in
    AND u.is_target = 1;
END
GO


























CREATE OR ALTER PROCEDURE dbo.get_schema
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    -- This prevents the count of affected rows from being sent to the client
    -- which is a good practice for performance.
    SET NOCOUNT ON;

    -- The main query to fetch the schema JSON
    SELECT ss.scraper_schema_json
    FROM dbo.scraper_schemas ss
    JOIN dbo.sources s ON s.source_id = ss.scraper_schema_source_id
    WHERE s.source_id = @source_id_in;
END
GO


CREATE OR ALTER PROCEDURE dbo.save_schema
(
    @source_id_in UNIQUEIDENTIFIER,
    @schema_json_in NVARCHAR(MAX)
)
AS
BEGIN
    -- This prevents the count of affected rows from being sent to the client.
    SET NOCOUNT ON;

    -- The MERGE statement to either insert a new schema or update an existing one.
    MERGE dbo.scraper_schemas AS target
    USING (SELECT @source_id_in AS source_id, @schema_json_in AS schema_json) AS source
    ON (target.scraper_schema_source_id = source.source_id)
    WHEN MATCHED THEN
        UPDATE SET
            scraper_schema_json = source.schema_json
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (scraper_schema_source_id, scraper_schema_json)
        VALUES (source.source_id, source.schema_json);
END
GO




CREATE OR ALTER PROCEDURE dbo.get_data
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    -- This prevents the count of affected rows from being sent to the client.
    SET NOCOUNT ON;

    -- The main query to fetch the top 100 course records.
    SELECT TOP 100
        c.course_code,
        c.course_title,
        c.course_description
    FROM
        dbo.courses c
    JOIN
        dbo.sources s ON s.source_id = c.course_source_id
    WHERE
        s.source_id = @source_id_in;
END
GO



CREATE OR ALTER PROCEDURE dbo.save_course_data
(
    @source_id_in UNIQUEIDENTIFIER,
    @course_data dbo.CourseData_v1 READONLY
)
AS
BEGIN
    SET NOCOUNT ON;

    -- The MERGE statement now uses the incoming table-valued parameter (@course_data)
    -- as its source, making the operation incredibly efficient.
    MERGE dbo.courses WITH(HOLDLOCK) AS target
    USING (
        SELECT
            @source_id_in AS sid,
            course_code AS code,
            course_title AS title,
            course_description AS description
        FROM
            @course_data
    ) AS source
    ON (target.course_source_id = source.sid
        AND COALESCE(target.course_code, '') = COALESCE(source.code, '')
        AND target.course_title = source.title)
    WHEN MATCHED THEN
        UPDATE SET
            course_description = source.description
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (course_source_id, course_code, course_title, course_description)
        VALUES (source.sid, source.code, source.title, source.description);
END
GO