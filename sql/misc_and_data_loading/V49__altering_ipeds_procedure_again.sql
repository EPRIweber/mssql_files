ALTER PROCEDURE find_similar_ipeds
  @tvpData source_tvp READONLY
AS
BEGIN
  SELECT DISTINCT td.source_name, u.uni_host FROM @tvpData td
  LEFT JOIN universities u
  ON u.instnm LIKE '%' + td.source_name + '%'
  OR td.source_name LIKE '%' + u.instnm + '%'
END
GO