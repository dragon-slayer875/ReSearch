package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const (
	CrawlerScoreBoard = "crawlerBoard:score"
	CrawlerTimeBoard  = "crawlerBoard:time"
)

func New(ctx context.Context, url string) (*redis.Client, error) {
	redisOpts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(redisOpts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return client, nil
}
