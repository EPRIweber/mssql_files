-- Flyway migration script V39 for MSSQL
-- Cleans and reformats the cipcode column in both the programs and completions_fact tables.
-- This version handles all key dependencies, de-duplicates data, and aligns data types before restoring keys.

PRINT 'Starting migration to clean and reformat cipcode column...';
GO

-- Step 1: Add temporary columns to both tables.
PRINT '-> Step 1: Adding temporary columns...';
ALTER TABLE dbo.programs ADD cipcode_temp DECIMAL(10, 4) NULL;
ALTER TABLE dbo.completions_fact ADD cipcode_temp DECIMAL(10, 4) NULL;
PRINT '  -> Temporary columns added successfully.';
GO


-- Step 2: Update the new columns by cleaning and casting the old varchar data.
PRINT '-> Step 2: Converting data from old columns to new columns...';
-- Update programs table
UPDATE dbo.programs
SET cipcode_temp = TRY_CAST(REPLACE(REPLACE(cipcode, '="', ''), '"', '') AS DECIMAL(10, 4))
WHERE cipcode LIKE '="%"';
UPDATE dbo.programs
SET cipcode_temp = TRY_CAST(cipcode AS DECIMAL(10, 4))
WHERE cipcode_temp IS NULL AND cipcode IS NOT NULL;
PRINT '  -> Data converted for programs table.';

-- Update completions_fact table
UPDATE dbo.completions_fact
SET cipcode_temp = TRY_CAST(REPLACE(REPLACE(cipcode, '="', ''), '"', '') AS DECIMAL(10, 4))
WHERE cipcode LIKE '="%"';
UPDATE dbo.completions_fact
SET cipcode_temp = TRY_CAST(cipcode AS DECIMAL(10, 4))
WHERE cipcode_temp IS NULL AND cipcode IS NOT NULL;
PRINT '  -> Data converted for completions_fact table.';
GO


-- Step 3: Drop the dependent Foreign Key and Primary Key constraints.
PRINT '-> Step 3: Dropping key constraints...';
ALTER TABLE dbo.completions_fact DROP CONSTRAINT fk_completions_programs;
PRINT '  -> Foreign key fk_completions_programs dropped.';
ALTER TABLE dbo.programs DROP CONSTRAINT PK__programs__DD149B39EB72A8AA;
PRINT '  -> Primary key on programs dropped.';
GO


-- Step 4: De-duplicate the data in the programs table based on the cleaned cipcode.
PRINT '-> Step 4: Removing duplicate records from the programs table...';
WITH CTE AS (
    SELECT
        cipcode_temp,
        rn = ROW_NUMBER() OVER (PARTITION BY cipcode_temp ORDER BY (SELECT NULL))
    FROM dbo.programs
)
DELETE FROM CTE WHERE rn > 1;
PRINT '  -> Duplicate records removed from programs.';
GO


-- Step 5: Remove any records from completions_fact that now point to a non-existent program.
PRINT '-> Step 5: Removing orphan records from completions_fact...';
DELETE FROM dbo.completions_fact
WHERE cipcode_temp IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dbo.programs WHERE dbo.programs.cipcode_temp = dbo.completions_fact.cipcode_temp);
PRINT '  -> Orphan records removed from completions_fact.';
GO


-- Step 6: Drop the old, messy cipcode columns from both tables.
PRINT '-> Step 6: Dropping old cipcode columns...';
ALTER TABLE dbo.programs DROP COLUMN cipcode;
ALTER TABLE dbo.completions_fact DROP COLUMN cipcode;
PRINT '  -> Old columns dropped successfully.';
GO


-- Step 7: Rename the temporary columns to the final name 'cipcode'.
PRINT '-> Step 7: Renaming temporary columns...';
EXEC sp_rename 'dbo.programs.cipcode_temp', 'cipcode', 'COLUMN';
EXEC sp_rename 'dbo.completions_fact.cipcode_temp', 'cipcode', 'COLUMN';
PRINT '  -> Columns renamed successfully.';
GO


-- Step 8: Enforce data integrity by setting the columns to NOT NULL.
PRINT '-> Step 8: Enforcing NOT NULL constraints...';
UPDATE dbo.programs SET cipcode = 0.0000 WHERE cipcode IS NULL;
ALTER TABLE dbo.programs ALTER COLUMN cipcode DECIMAL(10, 4) NOT NULL;
PRINT '  -> programs.cipcode column is now NOT NULL.';

UPDATE dbo.completions_fact SET cipcode = 0.0000 WHERE cipcode IS NULL;
ALTER TABLE dbo.completions_fact ALTER COLUMN cipcode DECIMAL(10, 4) NOT NULL;
PRINT '  -> completions_fact.cipcode column is now NOT NULL.';
GO


-- Step 9: Re-create the Primary Key with the newly formatted and de-duplicated column.
PRINT '-> Step 9: Re-creating primary key on programs...';
ALTER TABLE dbo.programs ADD CONSTRAINT PK_programs PRIMARY KEY (cipcode);
PRINT '  -> New primary key created successfully.';
GO


-- Step 10: Re-create the Foreign Key constraint on 'completions_fact' to restore the relationship.
PRINT '-> Step 10: Re-creating foreign key constraint on completions_fact...';
ALTER TABLE dbo.completions_fact ADD CONSTRAINT fk_completions_programs
    FOREIGN KEY (cipcode) REFERENCES dbo.programs(cipcode);
PRINT '  -> Foreign key fk_completions_programs re-created successfully.';
GO


PRINT 'Migration V39 complete.';
GO
