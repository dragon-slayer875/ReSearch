-- name: InsertMetadata :one
INSERT INTO metadata (url_id, title, meta_title, meta_description, meta_robots)
VALUES ($1, $2, $3, $4, $5)
RETURNING url_id, title, meta_title, meta_description, meta_robots;

-- name: UpsertMetadata :one
INSERT INTO metadata (url_id, title, meta_title, meta_description, meta_robots)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (url_id) DO UPDATE SET
    title = EXCLUDED.title,
    meta_title = EXCLUDED.meta_title,
    meta_description = EXCLUDED.meta_description,
    meta_robots = EXCLUDED.meta_robots
RETURNING url_id, title, meta_title, meta_description, meta_robots;

-- name: UpdateMetadata :one
UPDATE metadata 
SET title = $2, meta_title = $3, meta_description = $4, meta_robots = $5
WHERE url_id = $1
RETURNING url_id, title, meta_title, meta_description, meta_robots;

-- name: GetMetadataByURLID :one
SELECT url_id, title, meta_title, meta_description, meta_robots
FROM metadata
WHERE url_id = $1;

-- Inverted Index Queries
-- name: InsertInvertedIndex :one
INSERT INTO inverted_index (word, document_bits)
VALUES ($1, $2)
RETURNING word, document_bits;

-- name: UpsertInvertedIndex :one
INSERT INTO inverted_index (word, document_bits)
VALUES ($1, $2)
ON CONFLICT (word) DO UPDATE SET
    document_bits = EXCLUDED.document_bits
RETURNING word, document_bits;

-- name: UpdateInvertedIndex :one
UPDATE inverted_index 
SET document_bits = $2
WHERE word = $1
RETURNING word, document_bits;

-- name: GetInvertedIndexByWord :one
SELECT word, document_bits
FROM inverted_index
WHERE word = $1;

-- name: GetInvertedIndexByWords :many
SELECT word, document_bits
FROM inverted_index
WHERE word = ANY($1::text[]);

-- Word Data Queries
-- name: InsertWordData :one
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4)
RETURNING word, url_id, position_bits, term_frequency;

-- name: UpsertWordData :one
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4)
ON CONFLICT (word, url_id) DO UPDATE SET
    position_bits = EXCLUDED.position_bits,
    term_frequency = EXCLUDED.term_frequency
RETURNING word, url_id, position_bits, term_frequency;

-- name: UpdateWordData :one
UPDATE word_data 
SET position_bits = $3, term_frequency = $4
WHERE word = $1 AND url_id = $2
RETURNING word, url_id, position_bits, term_frequency;

-- name: GetWordDataByWordAndURL :one
SELECT word, url_id, position_bits, term_frequency
FROM word_data
WHERE word = $1 AND url_id = $2;

-- name: GetWordDataByURL :many
SELECT word, url_id, position_bits, term_frequency
FROM word_data
WHERE url_id = $1;

-- name: GetWordDataByWord :many
SELECT word, url_id, position_bits, term_frequency
FROM word_data
WHERE word = $1;

-- Batch operations for better performance
-- name: BatchInsertWordData :copyfrom
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- name: BatchUpsertWordData :exec
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
SELECT unnest($1::text[]), unnest($2::uuid[]), unnest($3::bytea[]), unnest($4::int[])
ON CONFLICT (word, url_id) DO UPDATE SET
    position_bits = EXCLUDED.position_bits,
    term_frequency = EXCLUDED.term_frequency;

-- Additional utility queries
-- name: GetWordCount :one
SELECT COUNT(*) FROM inverted_index;

-- name: GetDocumentWordCount :one
SELECT COUNT(*) FROM word_data WHERE url_id = $1;

-- name: GetWordFrequencySum :one
SELECT COALESCE(SUM(term_frequency), 0) FROM word_data WHERE url_id = $1;
