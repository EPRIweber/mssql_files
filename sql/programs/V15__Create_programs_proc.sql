-- Flyway migration script for MSSQL to create the upsert stored procedure for programs data.
-- This procedure uses the dbo.ProgramData_v1 table type.

CREATE PROCEDURE dbo.upsert_programs
    @data dbo.ProgramData_v1 READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO dbo.programs AS t  -- t = target
    USING @data AS s              -- s = source
    ON (t.cipcode = s.cipcode)

    -- If a program with the same cipcode already exists, update its details.
    WHEN MATCHED THEN
        UPDATE SET
            t.program_name = s.program_name,
            t.program_description = s.program_description

    -- If the program is new, insert it as a new row.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (cipcode, program_name, program_description)
        VALUES (s.cipcode, s.program_name, s.program_description);
END;