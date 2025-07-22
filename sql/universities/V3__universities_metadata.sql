-- Flyway migration script for MSSQL to create the 'universities' table.
-- This table holds the core, non-temporal directory and classification
-- information for each educational institution, primarily sourced from
-- the IPEDS HD2022 survey file.

CREATE TABLE universities (
    -- unitid is the unique IPEDS identifier for the institution.
    -- It serves as the primary key for linking all other data.
    unitid INT PRIMARY KEY,

    -- General directory information (from HD2022)
    instnm NVARCHAR(255) NOT NULL, -- Corresponds to INSTNM: Institution name
    addr NVARCHAR(255),            -- Corresponds to ADDR: Street address
    city NVARCHAR(100),            -- Corresponds to CITY: City
    stabbr NVARCHAR(2) NOT NULL,   -- Corresponds to STABBR: State abbreviation
    zip NVARCHAR(10),              -- Corresponds to ZIP: ZIP code
    webaddr NVARCHAR(255),         -- Corresponds to WEBADDR: Institution's website

    -- Institutional characteristics (from HD2022)
    control SMALLINT,              -- Corresponds to CONTROL: 1=Public, 2=Private non-profit, 3=Private for-profit
    sector SMALLINT,               -- Corresponds to SECTOR: Detailed sector of institution (e.g., 4-year, 2-year)
    c18basic SMALLINT,             -- Corresponds to C18BASIC: Carnegie Classification 2018 Basic

    -- Special designation flags (from HD2022)
    hbcu BIT                       -- Corresponds to HBCU: Historically Black College or University
);

-- Add comments to the table and columns for clarity in database tools using MSSQL extended properties.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Stores core directory and classification data for educational institutions from IPEDS.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Primary Key: Unique identification number of the institution from IPEDS.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'unitid';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Institution (or school) name.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'instnm';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Code for control of institution (1=Public, 2=Private non-profit, 3=Private for-profit).',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'control';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Code for sector of institution (e.g., 4-year, 2-year, public, private).',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'sector';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Code for Carnegie Classification 2018: Basic.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'c18basic';

EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Flag for Historically Black College or University status (1=Yes, 0=No).',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'hbcu';
