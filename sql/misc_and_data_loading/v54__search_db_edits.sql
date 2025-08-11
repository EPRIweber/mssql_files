ALTER TABLE distinct_sources
DROP COLUMN search_query;


ALTER TABLE search_results
ADD search_query NVARCHAR(500);