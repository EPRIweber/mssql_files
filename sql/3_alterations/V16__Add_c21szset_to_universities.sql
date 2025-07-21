-- Flyway migration script to add the c21szset column to the universities table.
-- This column represents the institution's size and setting classification.

ALTER TABLE dbo.universities
ADD c21szset SMALLINT NULL;

-- Add a comment for the new column for clarity.
EXEC sp_addextendedproperty
    @name = N'MS_Description', @value = N'Code for Carnegie Classification 2021: Size and Setting.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'universities',
    @level2type = N'COLUMN', @level2name = N'c21szset';
