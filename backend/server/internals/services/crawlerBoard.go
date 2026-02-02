package services

import (
	"context"
	"server/internals/storage/redis"
	"time"

	redisLib "github.com/redis/go-redis/v9"
)

type CrawlerBoardService struct {
	redisClient *redisLib.Client
}

func NewCrawlerBoardService(client *redisLib.Client) *CrawlerBoardService {
	return &CrawlerBoardService{
		client,
	}
}

func (cb *CrawlerBoardService) GetSubmissions(ctx context.Context) ([]redisLib.Z, error) {
	return cb.redisClient.ZRevRangeWithScores(ctx, redis.CrawlerScoreBoard, 0, 9).Result()
}

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, entries []string) error {
	pipe := cb.redisClient.TxPipeline()

	for _, entry := range entries {
		pipe.ZIncrBy(ctx, redis.CrawlerScoreBoard, 1, entry)
		pipe.ZAddNX(ctx, redis.CrawlerTimeBoard, redisLib.Z{
			Member: entry,
			Score:  float64(time.Now().Unix()),
		})
	}

	_, err := pipe.Exec(ctx)

	return err
}
