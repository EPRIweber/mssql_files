ALTER TABLE sources
ADD
  url_base_exclude NVARCHAR (200) DEFAULT NULL,
  url_exclude_patterns NVARCHAR (200) DEFAULT NULL;
GO

/* Get Enabled Sources for Crawling */
CREATE OR ALTER PROCEDURE dbo.get_enabled_sources
AS
SELECT
    source_id,
    source_name         AS name,
    source_type         AS type,
    source_base_url     AS root_url,
    source_schema_url   AS schema_url,
    include_external,
    source_crawl_depth  AS crawl_depth,
    page_timeout_s,
    max_concurrency,
    url_base_exclude,
    url_exclude_patterns
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
    @max_conc INT = NULL,
    @base_exclude NVARCHAR (200) = NULL,
    @exclude_ps NVARCHAR (200) = NULL
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
               @max_conc     AS max_conc,
               @base_exclude AS url_base_exclude,
               @exclude_ps   AS url_exclude_patterns) AS src
      ON tgt.source_name = src.name
    WHEN MATCHED THEN
        UPDATE SET
            tgt.source_type         = src.type,
            tgt.source_base_url     = src.base_url,
            tgt.source_schema_url   = src.schema_url,
            tgt.source_crawl_depth  = COALESCE(src.crawl_depth , tgt.source_crawl_depth),
            tgt.include_external    = COALESCE(src.include_ext  , tgt.include_external),
            tgt.page_timeout_s      = COALESCE(src.page_timeout , tgt.page_timeout_s),
            tgt.max_concurrency     = COALESCE(src.max_conc     , tgt.max_concurrency),
            tgt.url_base_exclude    = COALESCE(src.url_base_exclude , tgt.url_base_exclude),
            tgt.url_exclude_patterns= COALESCE(src.url_exclude_patterns , tgt.url_exclude_patterns)
    WHEN NOT MATCHED THEN
        INSERT (source_name, source_type, source_base_url, source_schema_url,
                source_crawl_depth, include_external, page_timeout_s, max_concurrency,
                url_base_exclude, url_exclude_patterns)
        VALUES (src.name, src.type, src.base_url, src.schema_url,
                src.crawl_depth, src.include_ext, src.page_timeout, src.max_conc,
                src.url_base_exclude, src.url_exclude_patterns);
    SELECT source_id FROM dbo.sources WHERE source_name = @name;
END;
GO
