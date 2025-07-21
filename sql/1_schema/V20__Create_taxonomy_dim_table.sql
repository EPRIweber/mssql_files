-- This script creates the 'taxonomy' table for storing classification data.
-- It is intended to be used as a Flyway migration.

CREATE TABLE dbo.taxonomy (
    -- Primary key for the taxonomy table, using a unique identifier that defaults to a new GUID.
    taxonomy_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    
    -- A text field to hold the description or name of the taxonomy item.
    -- It allows for long text and can be null.
    taxonomy_description NVARCHAR(MAX) DEFAULT NULL
);
