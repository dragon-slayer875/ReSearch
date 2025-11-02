-- Url Data Queries
-- name: InsertUrlData :exec
INSERT INTO url_data (url_id, title, description, raw_content)
VALUES ($1, $2, $3, $4);

-- Word Data Queries
-- name: InsertWordData :exec
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- Batch operations 
-- name: BatchInsertWordData :copyfrom
INSERT INTO word_data (word, url_id, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- name: UpdateTfIdf :exec
WITH total_docs AS (
    SELECT COUNT(DISTINCT url_id)::DOUBLE PRECISION as total 
    FROM word_data
),
word_stats AS (
    SELECT 
        word,
        COUNT(DISTINCT url_id) as doc_count
    FROM word_data
    GROUP BY word
)
UPDATE word_data wd
SET 
    idf = 1 + (ln((SELECT total FROM total_docs) / ws.doc_count::DOUBLE PRECISION)),
    tf_idf = term_frequency * (1+ ln((SELECT total FROM total_docs) / ws.doc_count::DOUBLE PRECISION))
FROM word_stats ws
WHERE wd.word = ws.word;
