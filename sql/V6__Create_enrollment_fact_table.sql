-- Flyway migration script for MSSQL to create the 'enrollment_fact' table.
-- This table stores temporal data on 12-month unduplicated student headcount.

CREATE TABLE enrollment_fact (
    enrollment_id INT IDENTITY(1,1) PRIMARY KEY,
    report_year INT NOT NULL,
    unitid INT NOT NULL,

    -- Foreign key constraint to link to the universities table.
    CONSTRAINT fk_enrollment_universities FOREIGN KEY (unitid) REFERENCES universities(unitid),

    -- Enrollment data points from EFFY files.
    student_level SMALLINT NOT NULL, -- Corresponds to EFFYALEV: Level of student (e.g., undergraduate, graduate)
    total_headcount INT             -- Corresponds to EFYTOTLT: 12-month unduplicated headcount
);

-- Create a non-clustered index for faster lookups by university and year.
CREATE INDEX ix_enrollment_fact_unitid_year ON enrollment_fact (unitid, report_year);

-- Add comments to the table and columns.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Fact table storing temporal data on 12-month unduplicated student headcount.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'enrollment_fact';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'The academic year for which the data is reported.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'enrollment_fact',
    @level2type = N'COLUMN', @level2name = N'report_year';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Foreign Key linking to the universities table.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'enrollment_fact',
    @level2type = N'COLUMN', @level2name = N'unitid';
