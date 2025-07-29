-- name: GetUrl :one
SELECT * FROM urls
WHERE id = $1 LIMIT 1;

-- name: ListUrls :many
SELECT * FROM urls
ORDER BY fetched_at DESC;

-- name: CreateUrls :copyfrom
INSERT INTO urls (
  id, url, crawl_status, fetched_at
) VALUES (
  $1, $2, $3, $4
);

-- name: UpdateUrlStatus :exec
UPDATE urls
  SET crawl_status = $2
WHERE url = $1;

-- name: DeleteUrl :exec
DELETE FROM urls
WHERE id = $1;
