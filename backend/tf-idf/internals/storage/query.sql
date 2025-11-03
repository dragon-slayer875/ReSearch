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
