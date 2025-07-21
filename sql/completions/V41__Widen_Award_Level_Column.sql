-- Flyway migration script V41 for MSSQL
-- Widens the award_level column in the completions_fact table and its dependent
-- objects from SMALLINT to INT to prevent numeric overflow errors.

PRINT 'Starting migration to widen award_level column...';
GO

-- Step 1: Drop the dependent stored procedure and table type.
-- These must be dropped before altering the column in the base table.
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


-- Step 2: Alter the column in the fact table from SMALLINT to INT.
PRINT '-> Step 2: Altering award_level column in completions_fact...';
ALTER TABLE dbo.completions_fact ALTER COLUMN award_level INT NOT NULL;
PRINT '  -> Column award_level successfully altered to INT.';
GO


-- Step 3: Re-create the User-Defined Table Type (UDTT) with the new data type.
PRINT '-> Step 3: Re-creating table type dbo.CompletionsData_v1...';

EXEC('
CREATE TYPE dbo.CompletionsData_v1 AS TABLE (
    report_year INT,
    unitid INT,
    cipcode DECIMAL(10, 4),
    award_level INT, -- Changed from SMALLINT to INT
    total_completions INT
);
');
PRINT '  -> Table type dbo.CompletionsData_v1 re-created successfully.';
GO


-- Step 4: Re-create the upsert stored procedure.
-- The procedure logic remains the same, but it now references the updated table type.
PRINT '-> Step 4: Re-creating stored procedure dbo.upsert_completions...';

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
PRINT '  -> Stored procedure dbo.upsert_completions re-created successfully.';
GO


PRINT 'Migration V41 complete.';
GO
