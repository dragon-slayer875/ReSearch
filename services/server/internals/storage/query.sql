-- name: GetSearchResultCount :one
SELECT COUNT(DISTINCT u.url) as total_results
FROM urls u 
JOIN word_data wd ON u.url = wd.url
WHERE wd.word = ANY($1::text[]);

-- name: GetSearchResults :many
SELECT u.url, u.title, u.description, u.content_summary, u.page_rank, COUNT(DISTINCT wd.word) as word_match_count, ARRAY_AGG(wd.word) matched_words, (0.5 * SUM(wd.tf_idf)::DOUBLE PRECISION + 0.5 * u.page_rank) AS combined_score
FROM urls u JOIN word_data wd
ON u.url = wd.url
WHERE wd.word = ANY($1::text[])
GROUP BY u.url ORDER BY word_match_count DESC, combined_score DESC LIMIT $2 OFFSET $3;

-- name: GetIndexedWords :many
SELECT DISTINCT word from word_data;
