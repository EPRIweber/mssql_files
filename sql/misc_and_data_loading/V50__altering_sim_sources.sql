ALTER PROCEDURE find_similar_sources
  @tvpData source_tvp READONLY
AS
BEGIN
  SELECT DISTINCT td.source_name, s.cleaned_name FROM @tvpData td
  LEFT JOIN sources s
  ON s.cleaned_name LIKE '%' + td.source_name + '%'
  OR td.source_name LIKE '%' + s.cleaned_name + '%'
  LEFT JOIN courses c
  ON c.course_source_id = s.source_id
  WHERE c.course_source_id IS NOT NULL
END
GO