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

-- name: BatchInsertLinks :batchexec
INSERT INTO links (
  "from", "to"
) VALUES (
  $1, $2
) ON CONFLICT ("from", "to")
DO NOTHING;
