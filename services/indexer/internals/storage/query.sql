-- Url Data Queries
-- name: InsertUrlData :exec
INSERT INTO urls (url, title, description, content_summary, crawled_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (url) DO UPDATE SET crawled_at = EXCLUDED.crawled_at;

-- Batch operations 
-- name: BatchInsertWordData :batchexec
INSERT INTO word_data (word, url, position_bits, term_frequency)
VALUES ($1, $2, $3, $4)
ON CONFLICT (word, url) DO UPDATE SET
    position_bits = EXCLUDED.position_bits,
    term_frequency = EXCLUDED.term_frequency;

-- name: BatchInsertLinks :batchexec
INSERT INTO links (
  "from", "to"
) VALUES (
  $1, $2
) ON CONFLICT ("from", "to")
DO NOTHING;
