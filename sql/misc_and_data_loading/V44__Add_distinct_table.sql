CREATE TABLE distinct_sources (
    distinct_id UNIQUEIDENTIFIER PRIMARY KEY NOT NULL DEFAULT NEWID(),
    distinct_name NVARCHAR(255) NOT NULL,
    distinct_scraper_status NVARCHAR(MAX) DEFAULT NULL,
    distinct_ipeds_unitid INT DEFAULT NULL
);
GO

ALTER TABLE sources ADD source_distinct_id UNIQUEIDENTIFIER
FOREIGN KEY (source_distinct_id) REFERENCES distinct_sources(distinct_id);
GO

INSERT INTO distinct_sources (distinct_name)
SELECT DISTINCT cleaned_name
FROM sources;

MERGE INTO dbo.sources AS s
USING dbo.distinct_sources AS ds
  ON ds.distinct_name = s.cleaned_name
WHEN MATCHED THEN
  UPDATE SET s.source_distinct_id = ds.distinct_id;
GO

CREATE OR ALTER TRIGGER dbo.trg_sources_after_insert
ON dbo.sources
AFTER INSERT
AS
BEGIN
  SET NOCOUNT ON;

  -- Insert any brand‑new cleaned_name values
  INSERT INTO dbo.distinct_sources (distinct_name)
  SELECT DISTINCT i.cleaned_name
  FROM inserted AS i
  WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.distinct_sources ds
    WHERE ds.distinct_name = i.cleaned_name
  );

  -- Link each new source row to its distinct_sources row
  UPDATE s
  SET s.source_distinct_id = ds.distinct_id
  FROM dbo.sources AS s
  INNER JOIN inserted AS i
    ON s.source_id = i.source_id
  INNER JOIN dbo.distinct_sources AS ds
    ON ds.distinct_name = i.cleaned_name;
END;
GO
