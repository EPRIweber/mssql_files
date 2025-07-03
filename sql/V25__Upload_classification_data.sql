-- -------------------------------------------------------------
-- Step 1: Create UDTT for passing course→taxonomy rows
-- -------------------------------------------------------------
CREATE TYPE dbo.CourseTaxonomyData_v1 AS TABLE (
  course_id   UNIQUEIDENTIFIER,
  taxonomy_id NVARCHAR(50)
);
GO

-- -------------------------------------------------------------
-- Step 2: Stored procedure to upsert into course_taxonomy01
-- -------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.save_course_taxonomy
  @taxonomy_data dbo.CourseTaxonomyData_v1 READONLY
AS
BEGIN
  SET NOCOUNT ON;
  MERGE dbo.course_taxonomy01 WITH (HOLDLOCK) AS target
  USING @taxonomy_data AS source
  ON target.course_id = source.course_id
  AND target.taxonomy_id = source.taxonomy_id
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (course_id, taxonomy_id)
    VALUES (source.course_id, source.taxonomy_id);
    -- (optional) WHEN NOT MATCHED BY SOURCE THEN DELETE;  -- if you want to remove old labels
END;
GO