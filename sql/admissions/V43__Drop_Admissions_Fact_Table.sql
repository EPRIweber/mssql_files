-- Flyway migration script V43 for MSSQL
-- Drops the admissions_fact table as it is no longer needed.
-- The associated index and foreign key constraint will be dropped automatically with the table.

PRINT 'Starting migration to drop admissions_fact table...';
GO

IF OBJECT_ID('dbo.admissions_fact', 'U') IS NOT NULL
BEGIN
    PRINT '-> Dropping table dbo.admissions_fact...';
    DROP TABLE dbo.admissions_fact;
    PRINT '  -> Table dbo.admissions_fact dropped successfully.';
END
ELSE
BEGIN
    PRINT '-> Table dbo.admissions_fact does not exist. Nothing to do.';
END
GO

PRINT 'Migration V43 complete.';
GO
