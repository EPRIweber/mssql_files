-- Flyway migration script for MSSQL to create objects for bulk-loading universities data.

-- Step 1: Create a User-Defined Table Type (UDTT) that matches the 'universities' table structure.
-- This type will be used as a parameter in our stored procedure to pass multiple rows at once.
CREATE TYPE dbo.UniversityData_v1 AS TABLE (
    unitid INT,
    instnm NVARCHAR(255),
    addr NVARCHAR(255),
    city NVARCHAR(100),
    stabbr NVARCHAR(2),
    zip NVARCHAR(10),
    webaddr NVARCHAR(255),
    control SMALLINT,
    sector SMALLINT,
    c18basic SMALLINT,
    hbcu BIT
);
