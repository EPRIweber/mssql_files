-- Step 4: Recreate the stored procedure with logic to handle the new column.
CREATE PROCEDURE dbo.upsert_universities
    @data dbo.UniversityData_v1 READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO dbo.universities AS t
    USING @data AS s
    ON (t.unitid = s.unitid)

    -- If a record matches, update all fields including the new one.
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
            t.hbcu = s.hbcu,
            t.c21szset = s.c21szset -- Update new column

    -- If no record matches, insert a new one including the new column.
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (unitid, instnm, addr, city, stabbr, zip, webaddr, control, sector, c18basic, hbcu, c21szset)
        VALUES (s.unitid, s.instnm, s.addr, s.city, s.stabbr, s.zip, s.webaddr, s.control, s.sector, s.c18basic, s.hbcu, s.c21szset);
END;
