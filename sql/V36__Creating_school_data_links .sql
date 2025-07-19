ALTER TABLE dbo.sources
    ADD
        cleaned_name AS (
            REPLACE(
                TRIM('_ ' FROM REPLACE(
                    REPLACE(LOWER(source_name), 'undergraduate', ''),
                'graduate', ''
            )), '_', ' ')
        ) PERSISTED,
        src_host AS (
            LOWER(
                REPLACE(
                    LEFT(
                        REPLACE(REPLACE(source_base_url,'http://',''),'https://','') + '/',
                        CHARINDEX('/',
                        REPLACE(REPLACE(source_base_url,'http://',''),'https://','') + '/'
                    )
                ), 'www.',''
            )
        )
    ) PERSISTED;
GO

ALTER TABLE dbo.universities
  ADD
    uni_host AS (
      LOWER(
        REPLACE(
          LEFT(
            REPLACE(REPLACE(webaddr,'http://',''),'https://','') + '/',
            CHARINDEX('/',
              REPLACE(REPLACE(webaddr,'http://',''),'https://','') + '/')
            ),
          'www.',''
        )
      )
    ) PERSISTED;
GO

CREATE NONCLUSTERED INDEX ix_sources_src_host
  ON dbo.sources(src_host);