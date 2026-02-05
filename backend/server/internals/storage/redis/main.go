package redis

import (
	"context"
	"slices"

	"github.com/gofiber/fiber/v3/log"
	"github.com/redis/go-redis/v9"
)

const (
	CrawlerScoreBoard  = "crawlerBoard:score"
	CrawlerTimeBoard   = "crawlerBoard:time"
	DomainPendingQueue = "crawl:domain_pending"
	CrawlerBoardIdx    = "crawlerboardIdx"
	CrawlerBoardPrefix = "cbidx:"
)

func New(ctx context.Context, url string) (*redis.Client, error) {
	redisOpts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	redisOpts.Protocol = 2

	client := redis.NewClient(redisOpts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	indexes, err := client.FT_List(ctx).Result()
	if err != nil {
		return nil, err
	}

	if !slices.Contains(indexes, CrawlerBoardIdx) {
		result, err := client.FTCreate(ctx, CrawlerBoardIdx, &redis.FTCreateOptions{
			OnHash: true,
			Prefix: []any{CrawlerBoardPrefix},
		}, &redis.FieldSchema{
			FieldName: "score",
			FieldType: redis.SearchFieldTypeNumeric,
			Sortable:  true,
		}, &redis.FieldSchema{
			FieldName: "time",
			FieldType: redis.SearchFieldTypeNumeric,
			Sortable:  true,
		}).Result()

		log.Info(result)

		if err != nil {
			return nil, err
		}
	}

	return client, nil
}
