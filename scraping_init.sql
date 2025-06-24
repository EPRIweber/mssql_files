/*  ============================================================
    Helper procedure: adds audit & guid columns + AFTER-UPDATE
    trigger to a single table if they do not already exist.
    ============================================================  */
CREATE OR ALTER PROCEDURE dbo.add_audit_columns
    @schema sysname,
    @tbl    sysname
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @fq nvarchar(260) = QUOTENAME(@schema) + N'.' + QUOTENAME(@tbl);

    /* ------- 1. add missing columns & DEFAULTs ------- */
    DECLARE @cols table(col sysname, def nvarchar(200) NULL);
    INSERT @cols(col,def)
    VALUES
      (@tbl + '_crtd_id', 'SUSER_SNAME()'),
      (@tbl + '_crtd_dt', 'SYSDATETIME()'),
      (@tbl + '_updt_id', NULL),                   -- set by trigger
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

    EXEC (@sql);     -- 1st batch: only ALTER TABLE statements

    /* ------- 2. AFTER UPDATE trigger ------- */
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



/*  ============================================================
    Database-level DDL trigger: calls helper after every
    successful CREATE TABLE.
    ============================================================  */
CREATE OR ALTER TRIGGER trg_after_create_table
ON DATABASE
AFTER CREATE_TABLE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ev   xml     = EVENTDATA();
    DECLARE @sch  sysname = @ev.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname');
    DECLARE @tbl  sysname = @ev.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname');

    -- ignore system/internal schemas
    IF @sch NOT IN ('sys', 'INFORMATION_SCHEMA') --, 'logs', 'log_stages')
        EXEC dbo.add_audit_columns @sch, @tbl;
END;
GO






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

CREATE TABLE urls(
    url_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    url_source_id UNIQUEIDENTIFIER NOT NULL,
    url_link NVARCHAR (300) NOT NULL,
    is_target BIT DEFAULT 1,
    CONSTRAINT urls_fk1 FOREIGN KEY (url_source_id)
    REFERENCES sources (source_id),
    CONSTRAINT urls_uq UNIQUE (url_source_id, url_link)
);

CREATE TABLE scraper_schemas(
    scraper_schema_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    scraper_schema_source_id UNIQUEIDENTIFIER NOT NULL,
    scraper_schema_json NVARCHAR (MAX) NOT NULL,
    CONSTRAINT scraper_schemas_fk1 FOREIGN KEY (scraper_schema_source_id)
    REFERENCES sources (source_id)
);

CREATE TABLE courses(
    course_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    course_source_id UNIQUEIDENTIFIER NOT NULL,
    course_title NVARCHAR (250) NOT NULL,
    course_description NVARCHAR (MAX) NOT NULL,
    course_code NVARCHAR (50) DEFAULT NULL,
    CONSTRAINT courses_fk1 FOREIGN KEY (course_source_id)
    REFERENCES sources (source_id)
);

CREATE TABLE runs(
    run_id INT PRIMARY KEY IDENTITY(1000, 1) NOT NULL,
    run_status NVARCHAR (20) NOT NULL,
    run_start_time DATETIME NOT NULL DEFAULT SYSDATETIME(),
    run_end_time DATETIME DEFAULT NULL
);

CREATE TABLE log_stages(
    stage_id TINYINT PRIMARY KEY NOT NULL,
    stage_name NVARCHAR(40) NOT NULL
);

INSERT INTO log_stages (stage_id, stage_name) VALUES
(0, 'crawl'), (1, 'schema'), (2, 'scrape'), (3, 'storage');

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


-- Create a user-defined table type to match the data being sent.
-- This defines the structure of the data we'll pass from Python.
CREATE TYPE dbo.CourseData_v1 AS TABLE (
    course_code NVARCHAR(255),
    course_title NVARCHAR(512) NOT NULL,
    course_description NVARCHAR(MAX)
);
GO