-- Flyway migration script for MSSQL to create the upsert stored procedure for universities data.
-- This procedure uses the dbo.UniversityData_v1 table type created in the previous migration.

CREATE PROCEDURE dbo.upsert_universities
    @data dbo.UniversityData_v1 READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO dbo.universities AS t  -- t = target
    USING @data AS s                  -- s = source
    ON (t.unitid = s.unitid)

    -- If a university with the same unitid already exists, update its fields.
    WHEN MATCHED THEN
        UPDATE SET
            t.instnm = s.instnm,
            t.addr = s.addr,
            t.city = s.city,
            t.stabbr = s.stabbr,
            t.zip = s.zip,
            t.webaddr = s.webaddr,
            t.control = s.control,
            t.sector = s.sector,
            t.c18basic = s.c18basic,
            t.hbcu = s.hbcu

    -- If the university does not exist in the target table, insert it as a new row.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (unitid, instnm, addr, city, stabbr, zip, webaddr, control, sector, c18basic, hbcu)
        VALUES (s.unitid, s.instnm, s.addr, s.city, s.stabbr, s.zip, s.webaddr, s.control, s.sector, s.c18basic, s.hbcu);
END;
