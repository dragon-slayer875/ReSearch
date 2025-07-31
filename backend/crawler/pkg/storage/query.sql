-- name: GetUrl :one
SELECT * FROM urls
WHERE url = $1 LIMIT 1;

-- name: CreateUrls :copyfrom
INSERT INTO urls (
  url, fetched_at
) VALUES (
  $1, $2
);

-- name: UpdateUrlStatus :exec
UPDATE urls
  SET fetched_at = $2
WHERE url = $1;

-- name: DeleteUrl :exec
DELETE FROM urls
WHERE url = $1;

-- name: CreateRobotRules :exec
INSERT INTO robot_rules (
  domain, rules, fetched_at
) VALUES (
  $1, $2, $3
);

-- name: GetRobotRules :one
SELECT * FROM robot_rules
WHERE domain = $1;

-- name: UpdateRobotRules :exec
UPDATE robot_rules
  SET rules = $2, fetched_at = $3
WHERE domain = $1;
