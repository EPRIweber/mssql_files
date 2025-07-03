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
        c.course_description
    FROM
        dbo.courses c
    JOIN
        dbo.sources s ON s.source_id = c.course_source_id
    WHERE
        s.source_id = @source_id_in;
END
GO