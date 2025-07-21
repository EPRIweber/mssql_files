-- V2__modify_logs_table.sql

ALTER TABLE dbo.logs
ADD
    log_level VARCHAR(10) NULL,      -- To store levels like 'INFO', 'ERROR', 'WARN'
    event_type VARCHAR(50) NULL,     -- A short, queryable event name like 'CrawlComplete'
    details NVARCHAR(MAX) NULL,      -- For full exceptions or multi-line details
    metric_name VARCHAR(50) NULL,    -- The name of a metric, e.g., 'UrlsFound'
    metric_value FLOAT NULL;         -- The numeric value of the metric
GO

-- After altering the table, we can make the log_message nullable,
-- as the core information will be in the new columns.
ALTER TABLE dbo.logs
ALTER COLUMN log_message NVARCHAR(MAX) NULL;
GO