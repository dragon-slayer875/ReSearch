-- Word Data Queries
-- name: GetWordDataByWordAndURL :one
SELECT *
FROM word_data
WHERE word = $1 AND url = $2;

-- name: GetWordDataByURL :many
SELECT *
FROM word_data
WHERE url = $1;

-- name: GetWordDataByWord :many
SELECT *
FROM word_data
WHERE word = $1;

-- name: GetSearchResultCount :one
SELECT COUNT(DISTINCT u.url) as total_results
FROM urls u 
JOIN word_data wd ON u.url = wd.url
WHERE wd.word = ANY($1::text[]);

-- name: GetSearchResults :many
SELECT u.url, u.title, u.description, u.content_summary, COUNT(DISTINCT wd.word) as word_match_count, ARRAY_AGG(wd.word) matched_words, SUM(wd.tf_idf)::DOUBLE PRECISION as total_relevance 
FROM urls u JOIN word_data wd
ON u.url = wd.url
WHERE wd.word = ANY($1::text[])
GROUP BY u.url ORDER BY word_match_count DESC, total_relevance DESC LIMIT $2 OFFSET $3;
