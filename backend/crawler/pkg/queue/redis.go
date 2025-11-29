package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DomainPendingQueue    = "crawl:domain_pending"
	DomainProcessingQueue = "crawl:domain_processing"
	UrlsProcessingQueue   = "crawl:urls_processing"
	IndexPendingQueue     = "index:pending"
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

	redisOpts.MaxRetries = 5
	redisOpts.MaxRetryBackoff = 10 * time.Second
	redisOpts.MinRetryBackoff = 1 * time.Second
	redisOpts.ReadTimeout = 5 * time.Second
	redisOpts.WriteTimeout = 5 * time.Second
	redisOpts.DialTimeout = 10 * time.Second

	redisClient := redis.NewClient(redisOpts)

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return redisClient, nil
}
