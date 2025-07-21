-- Flyway migration script V37 for MSSQL
-- Renames the 'grrtot' column to 'grtotlt' in the 'graduationrates_fact' table
-- and updates all dependent objects (UDTT and Stored Procedure) to use the new name.

-- Step 1: Drop the dependent objects (stored procedure and table type)
-- These must be dropped before renaming the column in the base table to avoid errors.
PRINT 'Dropping dependent stored procedure and table type...';

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'upsert_graduation_rates')
BEGIN
    DROP PROCEDURE dbo.upsert_graduation_rates;
    PRINT '  -> Stored procedure dbo.upsert_graduation_rates dropped.';
END;

IF TYPE_ID(N'dbo.GraduationRateData_v1') IS NOT NULL
BEGIN
    DROP TYPE dbo.GraduationRateData_v1;
    PRINT '  -> Table type dbo.GraduationRateData_v1 dropped.';
END;

-- Step 2: Rename the column in the fact table
-- This changes the column to match the source data from the Access files.
PRINT 'Renaming column in dbo.graduationrates_fact...';

EXEC sp_rename 'dbo.graduationrates_fact.grrtot', 'grtotlt', 'COLUMN';
PRINT '  -> Column renamed from grrtot to grtotlt.';


-- Step 3: Re-create the User-Defined Table Type (UDTT) with the new column name
-- Wrapped in EXEC() to ensure it runs in its own batch.
PRINT 'Re-creating table type with the new column name...';

EXEC('
CREATE TYPE dbo.GraduationRateData_v1 AS TABLE (
    report_year INT,
    unitid INT,
    grtype SMALLINT,
    grtotlt INT -- Updated column name
);
');
PRINT '  -> Table type dbo.GraduationRateData_v1 re-created successfully.';


-- Step 4: Re-create the upsert stored procedure using the new column name
-- Wrapped in EXEC() to ensure it runs in its own batch.
PRINT 'Re-creating stored procedure with the new column name...';

EXEC('
CREATE PROCEDURE dbo.upsert_graduation_rates
    @data dbo.GraduationRateData_v1 READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO dbo.graduationrates_fact AS t
    USING @data AS s
    ON (t.unitid = s.unitid AND t.report_year = s.report_year AND t.grtype = s.grtype)

    -- If a record for the same university, year, and cohort type exists, update the total completers.
    WHEN MATCHED THEN
        UPDATE SET
            t.grtotlt = s.grtotlt -- Updated column name

    -- If the record is new, insert it.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (report_year, unitid, grtype, grtotlt) -- Updated column name
        VALUES (s.report_year, s.unitid, s.grtype, s.grtotlt); -- Updated column name
END;
');

PRINT '  -> Stored procedure dbo.upsert_graduation_rates re-created successfully.';
PRINT 'Migration V37 complete.';
