DROP PROCEDURE save_course_data;
GO
DROP TYPE CourseData_v2;
GO

CREATE TYPE dbo.CourseData_v2 AS TABLE (
  course_code NVARCHAR(MAX),
  course_title NVARCHAR(MAX) NOT NULL,
  course_description NVARCHAR(MAX),
  course_credits NVARCHAR(MAX)
);
GO



/* Save Course Data from TVP */
CREATE OR ALTER PROCEDURE dbo.save_course_data
(
    @source_id_in UNIQUEIDENTIFIER,
    @course_data dbo.CourseData_v2 READONLY
)
AS
BEGIN
    SET NOCOUNT ON;
    MERGE dbo.courses WITH(HOLDLOCK) AS target
    USING (
        SELECT
            @source_id_in AS sid,
            course_code AS code,
            course_title AS title,
            course_description AS description,
            course_credits AS credits
        FROM
            @course_data
    ) AS source
    ON (target.course_source_id = source.sid
        AND COALESCE(target.course_code, '') = COALESCE(source.code, '')
        AND target.course_title = source.title)
    WHEN MATCHED THEN
        UPDATE SET
            course_description = source.description,
            course_title = source.title,
            course_code = source.code,
            course_credits = source.credits
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (course_source_id, course_code, course_title, course_description, course_credits)
        VALUES (source.sid, source.code, source.title, source.description, source.credits);
END;
GO

DECLARE @dcname sysname;
SELECT @dcname = dc.name
  FROM sys.default_constraints dc
  JOIN sys.columns c
    ON c.default_object_id = dc.object_id
  JOIN sys.tables t
    ON t.object_id = c.object_id
 WHERE t.name = 'courses'
   AND c.name = 'course_credits';

IF @dcname IS NOT NULL
  EXEC('ALTER TABLE dbo.courses DROP CONSTRAINT ' + @dcname);
GO


ALTER TABLE dbo.courses
ALTER COLUMN course_credits NVARCHAR(MAX);
GO
