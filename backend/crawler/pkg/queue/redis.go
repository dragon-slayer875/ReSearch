package queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

const (
	PendingQueue      = "crawl:pending"
	ProcessingQueue   = "crawl:processing"
	SeenSet           = "crawl:seen"
	IndexPendingQueue = "index:pending"
)

type Job struct {
	Url string `json:"url"`
	Id  int64  `json:"id"`
}

func NewRedisClient(redisUrl string) (*redis.Client, error) {
	redisOpts, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URI: %w", err)
	}
	redisClient := redis.NewClient(redisOpts)

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return redisClient, nil
}
