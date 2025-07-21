-- Flyway migration script V38 for MSSQL
-- Creates the User-Defined Table Type (UDTT) and the upsert stored procedure
-- for ingesting 12-month unduplicated student headcount data into enrollment_fact.

-- Step 1: Create the User-Defined Table Type (UDTT) for enrollment data.
-- This type matches the structure of the data we will be sending from Python.
PRINT 'Creating table type dbo.EnrollmentData_v1...';

EXEC('
CREATE TYPE dbo.EnrollmentData_v1 AS TABLE (
    report_year INT,
    unitid INT,
    student_level SMALLINT,
    total_headcount INT
);
');
PRINT '  -> Table type dbo.EnrollmentData_v1 created successfully.';


-- Step 2: Create the upsert stored procedure for enrollment data.
-- This procedure uses the MERGE statement to efficiently insert new records
-- or update existing ones based on university, year, and student level.
PRINT 'Creating stored procedure dbo.upsert_enrollment...';

EXEC('
CREATE PROCEDURE dbo.upsert_enrollment
    @data dbo.EnrollmentData_v1 READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO dbo.enrollment_fact AS t
    USING @data AS s
    ON (t.unitid = s.unitid AND t.report_year = s.report_year AND t.student_level = s.student_level)

    -- If a record for the same university, year, and student level exists, update the headcount.
    WHEN MATCHED THEN
        UPDATE SET
            t.total_headcount = s.total_headcount

    -- If the record is new, insert it.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (report_year, unitid, student_level, total_headcount)
        VALUES (s.report_year, s.unitid, s.student_level, s.total_headcount);
END;
');

PRINT '  -> Stored procedure dbo.upsert_enrollment created successfully.';
PRINT 'Migration V38 complete.';
