-- Flyway migration script V40 for MSSQL
-- Creates the User-Defined Table Type (UDTT) and the upsert stored procedure
-- for ingesting program completion data into completions_fact.

PRINT 'Starting migration to create completions data pipeline...';
GO

-- Step 1: Create the User-Defined Table Type (UDTT) for completions data.
-- The cipcode is DECIMAL(10, 4) to match the corrected programs table schema.
PRINT '-> Step 1: Creating table type dbo.CompletionsData_v1...';

EXEC('
CREATE TYPE dbo.CompletionsData_v1 AS TABLE (
    report_year INT,
    unitid INT,
    cipcode DECIMAL(10, 4),
    award_level SMALLINT,
    total_completions INT
);
');
PRINT '  -> Table type dbo.CompletionsData_v1 created successfully.';
GO


-- Step 2: Create the upsert stored procedure for completions data.
-- This procedure uses the MERGE statement to efficiently insert new records
-- or update existing ones based on a composite key.
PRINT '-> Step 2: Creating stored procedure dbo.upsert_completions...';

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
        t.cipcode = s.cipcode AND
        t.award_level = s.award_level
    )

    -- If a record for the same university, year, program, and award level exists, update the total.
    WHEN MATCHED THEN
        UPDATE SET
            t.total_completions = s.total_completions

    -- If the record is new, insert it.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (report_year, unitid, cipcode, award_level, total_completions)
        VALUES (s.report_year, s.unitid, s.cipcode, s.award_level, s.total_completions);
END;
');

PRINT '  -> Stored procedure dbo.upsert_completions created successfully.';
GO

PRINT 'Migration V40 complete.';
GO
