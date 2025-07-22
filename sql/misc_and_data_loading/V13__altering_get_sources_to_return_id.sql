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
    max_concurrency
FROM dbo.sources
WHERE is_enabled = 1;
GO