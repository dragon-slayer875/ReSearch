-- name: GetLinks :many
WITH discovered_urls AS (
	SELECT "from" AS url FROM links
	UNION
	SELECT "to" AS url FROM links
)
SELECT discovered_urls.url,
ARRAY_AGG( DISTINCT backlinks."from") AS backlinks,
COUNT(DISTINCT outlinks.to) AS outlinks_count
FROM discovered_urls
LEFT JOIN links AS backlinks ON discovered_urls.url = backlinks.to
LEFT JOIN links AS outlinks ON discovered_urls.url = outlinks."from"
GROUP BY discovered_urls.url;

-- name: UpdatePageRank :batchexec
UPDATE urls SET page_rank = $2
WHERE url = $1;
