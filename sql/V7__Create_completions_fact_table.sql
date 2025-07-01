-- Flyway migration script for MSSQL to create the 'completions_fact' table.
-- This table stores temporal data on degrees and certificates awarded by program.

CREATE TABLE completions_fact (
    completion_id INT IDENTITY(1,1) PRIMARY KEY,
    report_year INT NOT NULL,
    unitid INT NOT NULL,
    cipcode NVARCHAR(10) NOT NULL,

    -- Foreign key constraints
    CONSTRAINT fk_completions_universities FOREIGN KEY (unitid) REFERENCES universities(unitid),
    CONSTRAINT fk_completions_programs FOREIGN KEY (cipcode) REFERENCES programs(cipcode),

    -- Completions data points from C_A files
    award_level SMALLINT NOT NULL, -- Corresponds to AWLEVEL: The level of the award (e.g., Bachelor's, Master's)
    total_completions INT          -- Corresponds to CTOTALT: Total awards conferred
);

-- Create a non-clustered index for faster lookups.
CREATE INDEX ix_completions_fact_unitid_year ON completions_fact (unitid, report_year);

-- Add comments.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Fact table storing temporal data on degrees/certificates awarded by program.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'completions_fact';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Foreign Key linking to the programs table.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'completions_fact',
    @level2type = N'COLUMN', @level2name = N'cipcode';
