-- Flyway migration script for MSSQL to create a User-Defined Table Type (UDTT) for graduation rates data.
-- This type will be used to pass multiple graduation rate records at once.

CREATE TYPE dbo.GraduationRateData_v1 AS TABLE (
    report_year INT,
    unitid INT,
    grtype SMALLINT,
    grrtot INT
);
