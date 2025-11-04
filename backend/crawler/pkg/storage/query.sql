-- name: GetUrl :one
SELECT * FROM urls
WHERE url = $1 LIMIT 1;

-- name: InsertCrawledUrl :one
INSERT INTO urls (url) VALUES (
  $1
) ON CONFLICT (url)
DO UPDATE SET
	fetched_at = NOW()
RETURNING id;

-- name: InsertUrls :batchone
INSERT INTO urls (url) VALUES (
  $1
) ON CONFLICT (url)
DO UPDATE SET
	url = urls.url
RETURNING id;

-- name: DeleteUrl :exec
DELETE FROM urls
WHERE url = $1;

-- name: CreateRobotRules :exec
INSERT INTO robot_rules (
  domain, rules_json
) VALUES (
  $1, $2
) ON CONFLICT (domain)
DO UPDATE SET
	rules_json = EXCLUDED.rules_json,
	fetched_at = NOW();

-- name: GetRobotRules :one
SELECT * FROM robot_rules
WHERE domain = $1;

-- name: UpdateRobotRules :exec
UPDATE robot_rules
  SET rules_json = $2, fetched_at = $3
WHERE domain = $1;

-- name: BatchInsertLinks :batchexec
INSERT INTO links (
  "from", "to"
) VALUES (
  $1, $2
) ON CONFLICT ("from", "to")
DO NOTHING;
