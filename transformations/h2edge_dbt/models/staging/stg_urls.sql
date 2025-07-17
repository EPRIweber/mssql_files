-- models/staging/stg_urls.sql

SELECT
  url_id,
  url_source_id,
  url_link,
  is_target
FROM 
  {{ source('dbo', 'urls')}}