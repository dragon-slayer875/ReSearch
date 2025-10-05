package queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

const (
	PendingQueue           = "index:pending"
	ProcessingQueue        = "index:processing"
	PostingPendingQueue    = "post:pending"
	PostingProcessingQueue = "post:processing"
)

type IndexJob struct {
	JobId       int64  `json:"id"`
	Url         string `json:"url"`
	HtmlContent string `json:"html_content"`
	Timestamp   int64  `json:"timestamp"`
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
