-- Flyway migration script for MSSQL to create the 'programs' table.
-- This table stores information about academic programs, identified by their CIP code.

CREATE TABLE programs (
    -- CIPCODE is the Classification of Instructional Programs code.
    -- It's a standard code used to identify specific fields of study.
    cipcode NVARCHAR(10) PRIMARY KEY,

    -- Descriptive information about the program.
    program_name NVARCHAR(255) NOT NULL,
    program_description NVARCHAR(MAX)
);

-- Add comments to the table and columns.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Dimension table storing details for academic programs, keyed by CIP code.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'programs';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Primary Key: The 6-digit Classification of Instructional Programs (CIP) code.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'programs',
    @level2type = N'COLUMN', @level2name = N'cipcode';
