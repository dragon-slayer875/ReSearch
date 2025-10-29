-- Inverted Index Queries
-- name: InsertInvertedIndex :one
INSERT INTO inverted_index (word, document_bits, doc_frequency)
VALUES ($1, $2, $3)
RETURNING *;

-- name: BatchInsertInvertedIndex :batchone
INSERT INTO inverted_index (word, document_bits, doc_frequency)
VALUES ($1, $2, $3)
RETURNING *;

-- name: BatchUpdateInvertedIndexByWord :batchone
UPDATE inverted_index 
SET document_bits = $2,
    doc_frequency = $3
WHERE word = $1
RETURNING *;

-- name: BatchGetInvertedIndexByWord :batchone
SELECT *
FROM inverted_index
WHERE word = $1;

-- name: GetInvertedIndexByWords :many
SELECT * 
FROM inverted_index
WHERE word = ANY($1::text[]);

-- Url Data Queries
-- name: InsertUrlData :exec
INSERT INTO url_data (url_id, title, description, raw_content)
VALUES ($1, $2, $3, $4);

-- Word Data Queries
-- name: InsertWordData :exec
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- name: UpdateWordData :exec
UPDATE word_data 
SET position_bits = $3, term_frequency = $4,
    idf = $5, tf_idf = $6
WHERE word = $1 AND url_id = $2;

-- name: UpdateWordIdf :batchmany
UPDATE word_data 
SET idf = $2
WHERE word = $1
RETURNING word, url_id, term_frequency, idf;

-- name: UpdateWordTfIdf :batchone
UPDATE word_data
SET tf_idf = $3
WHERE word = $1 AND url_id = $2
RETURNING word, url_id, term_frequency, idf, tf_idf;

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

-- Batch operations 
-- name: BatchInsertInvertedIndexWithoutResult :copyfrom
INSERT INTO inverted_index (word, document_bits, doc_frequency)
VALUES ($1, $2, $3);

-- name: BatchInsertWordData :copyfrom
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- name: BatchInsertUrlData :copyfrom
INSERT INTO url_data (url_id, title, description, raw_content)
VALUES ($1, $2, $3, $4);

-- Additional utility queries
-- name: GetTotalIndexedDocumentCount :one
SELECT COUNT(DISTINCT url_id) FROM word_data;

-- name: GetWordCount :one
SELECT COUNT(*) FROM inverted_index;

-- name: GetDocumentWordCount :one
SELECT COUNT(*) FROM word_data WHERE url_id = $1;

-- name: GetWordFrequencySum :one
SELECT COALESCE(SUM(term_frequency), 0) FROM word_data WHERE url_id = $1;
