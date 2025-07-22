-- Flyway migration script for MSSQL to create the upsert stored procedure for graduation rates data.
-- This procedure uses the dbo.GraduationRateData_v1 table type.

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
            t.grrtot = s.grrtot

    -- If the record is new, insert it.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (report_year, unitid, grtype, grrtot)
        VALUES (s.report_year, s.unitid, s.grtype, s.grrtot);
END;
