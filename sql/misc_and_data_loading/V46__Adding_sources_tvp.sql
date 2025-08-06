CREATE TYPE source_tvp AS TABLE (
  source_name NVARCHAR (255) NOT NULL,
  source_base_url NVARCHAR (500) DEFAULT NULL
)
GO

CREATE PROCEDURE find_similar_sources
  @tvpData source_tvp READONLY
AS
BEGIN
  SELECT td.source_name, s.cleaned_name FROM @tvpData td
  LEFT JOIN sources s
  ON s.cleaned_name LIKE '%' + td.source_name + '%'
  OR td.source_name LIKE '%' + s.cleaned_name + '%'
END
GO

CREATE PROCEDURE find_similar_ipeds
  @tvpData source_tvp READONLY
AS
BEGIN
  SELECT td.source_name, u.instnm, u.uni_host FROM @tvpData td
  LEFT JOIN universities u
  ON u.instnm LIKE '%' + td.source_name + '%'
  OR td.source_name LIKE '%' + u.instnm + '%'
END
GO