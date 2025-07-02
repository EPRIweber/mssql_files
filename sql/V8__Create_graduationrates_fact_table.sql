-- Flyway migration script for MSSQL to create the 'graduationrates_fact' table.
-- This table stores temporal data on student cohort graduation rates.

CREATE TABLE graduationrates_fact (
    graduationrate_id INT IDENTITY(1,1) PRIMARY KEY,
    report_year INT NOT NULL,
    unitid INT NOT NULL,

    -- Foreign key constraint
    CONSTRAINT fk_gradrates_universities FOREIGN KEY (unitid) REFERENCES universities(unitid),

    -- Graduation rate data points from GR2022 file
    grtype SMALLINT NOT NULL,  -- Corresponds to GRTYPE: The specific student cohort (e.g., Bachelor's degree-seeking)
    grrtot INT                 -- Corresponds to GRRTOT: Grand total completers within 150% of normal time
);

-- Create a non-clustered index for faster lookups.
CREATE INDEX ix_graduationrates_fact_unitid_year ON graduationrates_fact (unitid, report_year);

-- Add comments.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Fact table storing temporal data on student cohort graduation rates.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'graduationrates_fact';
