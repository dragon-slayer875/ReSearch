-- name: GetUrl :one
SELECT * FROM urls
WHERE id = $1 LIMIT 1;

-- name: ListUrls :many
SELECT * FROM urls
ORDER BY fetched_at DESC;

-- name: CreateUrls :copyfrom
INSERT INTO urls (
  id, url, status_code, fetched_at
) VALUES (
  $1, $2, $3, $4
);

-- name: UpdateUrl :exec
UPDATE urls
  set url = $2,
  status_code = $3
WHERE id = $1
RETURNING *;

-- name: DeleteUrl :exec
DELETE FROM urls
WHERE id = $1;
