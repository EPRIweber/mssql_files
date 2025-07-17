-- models/staging/stg_scraper_schemas.sql

SELECT
  scraper_schema_id,
  scraper_schema_source_id,
  scraper_schema_json
FROM
  {{ source('dbo', 'scraper_schemas')}}