
DECLARE @dcname sysname;
SELECT @dcname = dc.name
  FROM sys.default_constraints dc
  JOIN sys.columns c
    ON c.default_object_id = dc.object_id
  JOIN sys.tables t
    ON t.object_id = c.object_id
 WHERE t.name = 'courses'
   AND c.name = 'course_title';

IF @dcname IS NOT NULL
  EXEC('ALTER TABLE dbo.courses DROP CONSTRAINT ' + @dcname);
GO


ALTER TABLE dbo.courses
ALTER COLUMN course_title NVARCHAR(MAX);
GO




DECLARE @dcname sysname;
SELECT @dcname = dc.name
  FROM sys.default_constraints dc
  JOIN sys.columns c
    ON c.default_object_id = dc.object_id
  JOIN sys.tables t
    ON t.object_id = c.object_id
 WHERE t.name = 'courses'
   AND c.name = 'course_code';

IF @dcname IS NOT NULL
  EXEC('ALTER TABLE dbo.courses DROP CONSTRAINT ' + @dcname);
GO


ALTER TABLE dbo.courses
ALTER COLUMN course_code NVARCHAR(MAX);
GO
