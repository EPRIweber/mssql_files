-- Flyway migration script to recreate the helper objects for the universities table
-- to include the new c21szset column.

-- Step 1: Drop the existing procedure that depends on the table type.
DROP PROCEDURE dbo.upsert_universities;

-- Step 2: Drop the existing table type.
DROP TYPE dbo.UniversityData_v1;

-- Step 3: Recreate the table type with the new c21szset column.
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
    hbcu BIT,
    c21szset SMALLINT -- New column added
);
