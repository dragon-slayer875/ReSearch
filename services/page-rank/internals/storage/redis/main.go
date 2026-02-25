package redis

import (
	"context"
	"page-rank/internals/retry"
	"page-rank/internals/utils"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client  *redis.Client
	retryer *retry.Retryer
}

func New(ctx context.Context, redisUrl string, retryer *retry.Retryer) (*RedisClient, error) {
	var client *redis.Client

	err := retryer.Do(ctx, func() error {
		redisOpts, err := redis.ParseURL(redisUrl)
		if err != nil {
			return err
		}

		client = redis.NewClient(redisOpts)

		if err := client.Ping(ctx).Err(); err != nil {
			return err
		}

		return nil
	}, utils.IsRetryableRedisError)

	if err != nil {
		return nil, err
	}

	return &RedisClient{
		client,
		retryer,
	}, nil
}

func (rc *RedisClient) Close() error {
	err := rc.retryer.Do(context.Background(), func() error {
		return rc.Client.Close()
	}, utils.IsRetryableRedisError)

	return err
}
