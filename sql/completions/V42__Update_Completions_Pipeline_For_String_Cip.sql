-- Flyway migration script V42 for MSSQL
-- Updates the completions data pipeline to accept cipcode as a string (NVARCHAR)
-- to bypass a pyodbc driver issue with the DECIMAL type.

PRINT 'Starting migration to update completions data pipeline...';
GO

-- Step 1: Drop the dependent stored procedure and table type.
PRINT '-> Step 1: Dropping dependent objects...';

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'upsert_completions')
BEGIN
    DROP PROCEDURE dbo.upsert_completions;
    PRINT '  -> Stored procedure dbo.upsert_completions dropped.';
END;
GO

IF TYPE_ID(N'dbo.CompletionsData_v1') IS NOT NULL
BEGIN
    DROP TYPE dbo.CompletionsData_v1;
    PRINT '  -> Table type dbo.CompletionsData_v1 dropped.';
END;
GO


-- Step 2: Re-create the User-Defined Table Type (UDTT) with cipcode as NVARCHAR.
PRINT '-> Step 2: Re-creating table type dbo.CompletionsData_v1...';

EXEC('
CREATE TYPE dbo.CompletionsData_v1 AS TABLE (
    report_year INT,
    unitid INT,
    cipcode NVARCHAR(10), -- Changed from DECIMAL to NVARCHAR
    award_level INT,
    total_completions INT
);
');
PRINT '  -> Table type dbo.CompletionsData_v1 re-created successfully.';
GO


-- Step 3: Re-create the upsert stored procedure.
-- The procedure now accepts a string for cipcode and relies on SQL Server's
-- implicit conversion when merging into the DECIMAL column of the final table.
PRINT '-> Step 3: Re-creating stored procedure dbo.upsert_completions...';

EXEC('
CREATE PROCEDURE dbo.upsert_completions
    @data dbo.CompletionsData_v1 READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO dbo.completions_fact AS t
    USING @data AS s
    ON (
        t.unitid = s.unitid AND
        t.report_year = s.report_year AND
        t.cipcode = s.cipcode AND -- SQL Server will handle the string-to-decimal conversion here
        t.award_level = s.award_level
    )

    WHEN MATCHED THEN
        UPDATE SET
            t.total_completions = s.total_completions

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (report_year, unitid, cipcode, award_level, total_completions)
        VALUES (s.report_year, s.unitid, s.cipcode, s.award_level, s.total_completions);
END;
');
PRINT '  -> Stored procedure dbo.upsert_completions re-created successfully.';
GO


PRINT 'Migration V42 complete.';
GO
