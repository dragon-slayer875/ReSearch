-- name: GetMetadataByURLID :one
SELECT *
FROM metadata
WHERE url_id = $1;

-- Inverted Index Queries
-- name: BatchGetInvertedIndexByWord :batchone
SELECT word, document_bits
FROM inverted_index
WHERE word = $1;

-- name: GetInvertedIndexByWords :many
SELECT * 
FROM inverted_index
WHERE word = ANY($1::text[]);

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
SELECT u.id, u.url, COUNT(DISTINCT wd.word) as word_match_count, ARRAY_AGG(wd.word) matched_words, SUM(wd.tf_idf)::DOUBLE PRECISION as total_relevance 
FROM urls u JOIN word_data wd
ON u.id = wd.url_id
WHERE wd.word = ANY($1::text[])
GROUP BY u.id
ORDER BY word_match_count DESC, total_relevance DESC;

-- Additional utility queries
-- name: GetTotalIndexedDocumentCount :one
SELECT COUNT(DISTINCT url_id) FROM word_data;

-- name: GetWordCount :one
SELECT COUNT(*) FROM inverted_index;

-- name: GetDocumentWordCount :one
SELECT COUNT(*) FROM word_data WHERE url_id = $1;

-- name: GetWordFrequencySum :one
SELECT COALESCE(SUM(term_frequency), 0) FROM word_data WHERE url_id = $1;
