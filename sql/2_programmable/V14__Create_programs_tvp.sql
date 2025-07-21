-- Flyway migration script for MSSQL to create a User-Defined Table Type (UDTT) for programs data.
-- This type will be used to pass multiple program records at once to a stored procedure.

CREATE TYPE dbo.ProgramData_v1 AS TABLE (
    cipcode NVARCHAR(10),
    program_name NVARCHAR(255),
    program_description NVARCHAR(MAX)
);
