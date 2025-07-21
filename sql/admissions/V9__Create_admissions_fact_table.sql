-- Flyway migration script for MSSQL to create the 'admissions_fact' table.
-- This table stores temporal data on university admissions.

CREATE TABLE admissions_fact (
    admission_id INT IDENTITY(1,1) PRIMARY KEY,
    report_year INT NOT NULL,
    unitid INT NOT NULL,

    -- Foreign key constraint
    CONSTRAINT fk_admissions_universities FOREIGN KEY (unitid) REFERENCES universities(unitid),

    -- Admissions data points from ADM files
    applicants_total INT,           -- Corresponds to APPLCN: Total applicants
    admissions_total INT,           -- Corresponds to ADMSSN: Total admissions
    enrollees_total INT             -- Corresponds to ENRLT: Total enrollees
);

-- Create a non-clustered index for faster lookups.
CREATE INDEX ix_admissions_fact_unitid_year ON admissions_fact (unitid, report_year);

-- Add comments.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Fact table storing temporal data on university admissions.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'admissions_fact';
