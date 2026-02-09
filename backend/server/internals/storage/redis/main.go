package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const (
	CrawlerboardKey    = "crawlerboard"
	DomainPendingQueue = "crawl:domain_pending"
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

	return client, nil
}
