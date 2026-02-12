-- Url Data Queries
-- name: InsertUrlData :exec
INSERT INTO urls (url, title, description, content_summary, crawled_at)
VALUES ($1, $2, $3, $4, $5);

-- Word Data Queries
-- name: InsertWordData :exec
INSERT INTO word_data (word, url, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- Batch operations 
-- name: BatchInsertWordData :copyfrom
INSERT INTO word_data (word, url, position_bits, term_frequency)
VALUES ($1, $2, $3, $4);

-- name: BatchInsertLinks :batchexec
INSERT INTO links (
  "from", "to"
) VALUES (
  $1, $2
) ON CONFLICT ("from", "to")
DO NOTHING;
