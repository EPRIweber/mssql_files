CREATE TYPE dbo.CourseData_v2 AS TABLE (
  course_code NVARCHAR(255),
  course_title NVARCHAR(512) NOT NULL,
  course_description NVARCHAR(MAX),
  course_credits NVARCHAR(200)
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



/* Get Course Data for a Source */
CREATE OR ALTER PROCEDURE dbo.get_data
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        c.course_id,
        c.course_code,
        c.course_title,
        c.course_description,
        c.course_credits
    FROM
        dbo.courses c
    JOIN
        dbo.sources s ON s.source_id = c.course_source_id
    WHERE
        s.source_id = @source_id_in;
END;
GO

DROP TYPE CourseData_v1;
GO