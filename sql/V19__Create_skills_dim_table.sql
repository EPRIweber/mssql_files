-- This script creates the 'skills' table.
-- It includes a unique identifier for the primary key and a description column.

CREATE TABLE skills (
    -- Primary key for the skills table, using a unique identifier that defaults to a new GUID.
    skill_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    
    -- A text field to hold the description of the skill.
    -- It allows for long text and can be null.
    skill_description NVARCHAR(MAX) DEFAULT NULL
);
