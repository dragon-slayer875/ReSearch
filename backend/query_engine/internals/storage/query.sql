-- Word Data Queries
-- name: GetWordDataByWordAndURL :one
SELECT *
FROM word_data
WHERE word = $1 AND url_id = $2;

-- name: GetWordDataByURL :many
SELECT *
FROM word_data
WHERE url_id = $1;

-- name: GetWordDataByWord :many
SELECT *
FROM word_data
WHERE word = $1;

-- name: GetSearchResults :many
SELECT u.id, u.url, ud.title, ud.description, ud.raw_content, COUNT(DISTINCT wd.word) as word_match_count, ARRAY_AGG(wd.word) matched_words, SUM(wd.tf_idf)::DOUBLE PRECISION as total_relevance 
FROM urls u JOIN word_data wd
ON u.id = wd.url_id
JOIN url_data ud
ON u.id = ud.url_id
WHERE wd.word = ANY($1::text[])
GROUP BY u.id, ud.title, ud.description, ud.raw_content
ORDER BY word_match_count DESC, total_relevance DESC;
