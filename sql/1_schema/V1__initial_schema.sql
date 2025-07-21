/*
    ================================================================
    Flyway Baseline Script: V1__initial_schema.sql
    ================================================================
    This script contains the complete initial schema for the H2EDGE database.
    It combines all CREATE statements for tables, types, procedures, and triggers.
*/

-- ================================================================
-- SECTION 1: USER-DEFINED TABLE TYPES
-- ================================================================

-- This defines the structure of the data we'll pass from Python.
CREATE TYPE dbo.CourseData_v1 AS TABLE (
    course_code NVARCHAR(255),
    course_title NVARCHAR(512) NOT NULL,
    course_description NVARCHAR(MAX)
);
GO

-- ================================================================
-- SECTION 2: TABLES
-- ================================================================

CREATE TABLE sources(
	source_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    source_name NVARCHAR (50) NOT NULL,
    source_type NVARCHAR (15) NOT NULL,
    is_enabled BIT NOT NULL DEFAULT 1,
    source_base_url NVARCHAR (200) DEFAULT NULL,
    source_schema_url NVARCHAR (200) DEFAULT NULL,
    include_external BIT DEFAULT NULL,
    source_crawl_depth INT DEFAULT NULL,
    page_timeout_s INT DEFAULT NULL,
    max_concurrency INT DEFAULT NULL,
    CONSTRAINT sources_uq UNIQUE (source_name, source_type)
);
GO

CREATE TABLE urls(
    url_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    url_source_id UNIQUEIDENTIFIER NOT NULL,
    url_link NVARCHAR (300) NOT NULL,
    is_target BIT DEFAULT 1,
    CONSTRAINT urls_fk1 FOREIGN KEY (url_source_id)
    REFERENCES sources (source_id),
    CONSTRAINT urls_uq UNIQUE (url_source_id, url_link)
);
GO

CREATE TABLE scraper_schemas(
    scraper_schema_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    scraper_schema_source_id UNIQUEIDENTIFIER NOT NULL,
    scraper_schema_json NVARCHAR (MAX) NOT NULL,
    CONSTRAINT scraper_schemas_fk1 FOREIGN KEY (scraper_schema_source_id)
    REFERENCES sources (source_id)
);
GO

CREATE TABLE courses(
    course_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    course_source_id UNIQUEIDENTIFIER NOT NULL,
    course_title NVARCHAR (250) DEFAULT NULL,
    course_description NVARCHAR (MAX) DEFAULT NULL,
    course_code NVARCHAR (50) DEFAULT NULL,
    CONSTRAINT courses_fk1 FOREIGN KEY (course_source_id)
    REFERENCES sources (source_id)
);
GO

CREATE TABLE runs(
    run_id INT PRIMARY KEY IDENTITY(1000, 1) NOT NULL,
    run_status NVARCHAR (20) NOT NULL,
    run_start_time DATETIME NOT NULL DEFAULT SYSDATETIME(),
    run_end_time DATETIME DEFAULT NULL
);
GO

CREATE TABLE log_stages(
    stage_id TINYINT PRIMARY KEY NOT NULL,
    stage_name NVARCHAR(40) NOT NULL
);
GO

CREATE TABLE logs(
    log_id INT IDENTITY(1000, 1) NOT NULL,
    log_run_id INT NOT NULL,
    log_source_id UNIQUEIDENTIFIER NOT NULL,
    log_stage TINYINT NOT NULL,
    log_message NVARCHAR (MAX) NOT NULL,
    log_ts DATETIME NOT NULL,
    PRIMARY KEY (log_id),
    CONSTRAINT logs_fk1 FOREIGN KEY (log_run_id)
    REFERENCES runs (run_id),
    CONSTRAINT logs_fk2 FOREIGN KEY (log_source_id)
    REFERENCES sources (source_id),
    CONSTRAINT logs_fk3 FOREIGN KEY (log_stage)
    REFERENCES log_stages (stage_id)
);
GO

-- ================================================================
-- SECTION 3: STATIC DATA
-- ================================================================

INSERT INTO log_stages (stage_id, stage_name) VALUES
(0, 'crawl'), (1, 'schema'), (2, 'scrape'), (3, 'storage');
GO

-- ================================================================
-- SECTION 4: PROCEDURES & TRIGGERS
-- ================================================================

/* Helper procedure: adds audit & guid columns + AFTER-UPDATE trigger */
CREATE OR ALTER PROCEDURE dbo.add_audit_columns
    @schema sysname,
    @tbl    sysname
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @fq nvarchar(260) = QUOTENAME(@schema) + N'.' + QUOTENAME(@tbl);
    DECLARE @cols table(col sysname, def nvarchar(200) NULL);
    INSERT @cols(col,def)
    VALUES
      (@tbl + '_crtd_id', 'SUSER_SNAME()'),
      (@tbl + '_crtd_dt', 'SYSDATETIME()'),
      (@tbl + '_updt_id', NULL),
      (@tbl + '_updt_dt', NULL),
      (@tbl + '_table_id', 'NEWID()');
    DECLARE @sql nvarchar(max) = N'';
    SELECT @sql += N'
IF NOT EXISTS (SELECT 1
               FROM sys.columns
               WHERE object_id = OBJECT_ID(''' + @fq + N''')
                 AND name = ''' + col + N''')
BEGIN
    ALTER TABLE ' + @fq + N'
    ADD ' + QUOTENAME(col) +
          CASE WHEN def IS NOT NULL
               THEN N' NVARCHAR(40) NOT NULL
                    CONSTRAINT DF_' + col + N' DEFAULT (' + def + N')'
               ELSE N' NVARCHAR(40) NULL'
          END + N';
END;'
    FROM @cols;
    EXEC (@sql);
    IF NOT EXISTS (SELECT 1
                   FROM sys.triggers
                   WHERE name = 'trg_upd_' + @tbl
                     AND parent_id = OBJECT_ID(@fq))
    BEGIN
        DECLARE @trg nvarchar(max) = N'
CREATE TRIGGER ' + QUOTENAME('trg_upd_' + @tbl) + N'
ON ' + @fq + N'
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM inserted)
    UPDATE t
       SET ' + QUOTENAME(@tbl + '_updt_id') + N' = SUSER_SNAME(),
           ' + QUOTENAME(@tbl + '_updt_dt') + N' = SYSDATETIME()
    FROM ' + @fq + N' t
    JOIN inserted i
      ON i.' + QUOTENAME(@tbl + '_table_id') + N' = t.' + QUOTENAME(@tbl + '_table_id') + N';
END;';
        EXEC (@trg);
    END
END
GO

/* Database-level DDL trigger: calls helper after every successful CREATE TABLE */
CREATE OR ALTER TRIGGER trg_after_create_table
ON DATABASE
AFTER CREATE_TABLE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ev   xml     = EVENTDATA();
    DECLARE @sch  sysname = @ev.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname');
    DECLARE @tbl  sysname = @ev.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname');
    IF @sch NOT IN ('sys', 'INFORMATION_SCHEMA')
        EXEC dbo.add_audit_columns @sch, @tbl;
END;
GO

/* Start Scraping Job "Lock" */
CREATE OR ALTER PROCEDURE dbo.begin_run
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRAN;
        IF EXISTS (SELECT 1 FROM runs WHERE run_status = 'running')
        BEGIN
            SELECT CAST(NULL AS INT) AS run_id;
            ROLLBACK TRAN;
            RETURN;
        END
        INSERT runs(run_status) VALUES('running');
        DECLARE @id INT = SCOPE_IDENTITY();
        COMMIT TRAN;
    SELECT @id AS run_id;
END;
GO

/* End Scraping Job "Lock" */
CREATE OR ALTER PROCEDURE dbo.end_run
(
    @run_id INT
)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE runs
       SET run_status = 'finished',
           run_end_time = SYSUTCDATETIME()
     WHERE run_id = @run_id;
END;
GO

/* Save Course Data from TVP */
CREATE OR ALTER PROCEDURE dbo.save_course_data
(
    @source_id_in UNIQUEIDENTIFIER,
    @course_data dbo.CourseData_v1 READONLY
)
AS
BEGIN
    SET NOCOUNT ON;
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

/* Get Schema for a Source */
CREATE OR ALTER PROCEDURE dbo.get_schema
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT ss.scraper_schema_json
    FROM dbo.scraper_schemas ss
    JOIN dbo.sources s ON s.source_id = ss.scraper_schema_source_id
    WHERE s.source_id = @source_id_in;
END
GO

/* Get Enabled Sources for Crawling */
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

/* Upsert a Source */
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

/* Get Target URLs for a Source */
CREATE OR ALTER PROCEDURE get_target_urls
(
	@source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    SET NOCOUNT ON
	SELECT u.url_link
    FROM urls u
    JOIN sources s ON s.source_id = u.url_source_id
    WHERE s.source_id = @source_id_in
    AND u.is_target = 1;
END
GO

/* Save Schema for a Source */
CREATE OR ALTER PROCEDURE dbo.save_schema
(
    @source_id_in UNIQUEIDENTIFIER,
    @schema_json_in NVARCHAR(MAX)
)
AS
BEGIN
    SET NOCOUNT ON;
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

/* Get Course Data for a Source */
CREATE OR ALTER PROCEDURE dbo.get_data
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    SET NOCOUNT ON;
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