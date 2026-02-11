package redis

import (
	"context"
	"crawler/internals/retry"
	"crawler/internals/utils"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DomainPendingQueue = "crawl:domain_pending"
	IndexPendingQueue  = "index:pending"
	FreshHashKey       = "crawl:fresh"
	DomainDelayHashKey = "crawl:domain_delays"
	CrawlQueuePrefix   = "cq:"
)

type hashedPage struct {
	Url         string   `redis:"url"`
	HtmlContent string   `redis:"html_content"`
	Timestamp   int64    `redis:"timestamp"`
	Outlinks    []string `redis:"outlinks"`
}

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

func NewWithClient(client *redis.Client, retryer *retry.Retryer) *RedisClient {
	return &RedisClient{
		client,
		retryer,
	}
}

func (rc *RedisClient) Close() error {
	err := rc.retryer.Do(context.Background(), func() error {
		return rc.Client.Close()
	}, utils.IsRetryableRedisError)

	return err
}

func (rc *RedisClient) ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.ZAddNX(ctx, key, members...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.ZAdd(ctx, key, members...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.ZRem(ctx, key, members...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	var redisCmd *redis.ZWithKeyCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.BZPopMin(ctx, timeout, keys...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	var redisCmd *redis.StringSliceCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.BRPop(ctx, timeout, keys...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) Pipeline() redis.Pipeliner {
	return rc.Client.Pipeline()
}

func (rc *RedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	var redisCmd *redis.StringCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.Get(ctx, key)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	var redisCmd *redis.StatusCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.Set(ctx, key, value, expiration)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) HSet(ctx context.Context, key string, values ...any) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.HSet(ctx, key, values...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) HGet(ctx context.Context, key string, field string) *redis.StringCmd {
	var redisCmd *redis.StringCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.HGet(ctx, key, field)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) HExists(ctx context.Context, key string, field string) *redis.BoolCmd {
	var redisCmd *redis.BoolCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.HExists(ctx, key, field)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *RedisClient) UpdateRedis(ctx context.Context, page *utils.CrawledPage) error {
	pipe := rc.Client.TxPipeline()

	// Update domain crawl delay and set page as fresh
	pipe.HSet(ctx, DomainDelayHashKey, page.Domain, time.Now().Unix())
	pipe.HSetEXWithArgs(ctx, FreshHashKey, &redis.HSetEXOptions{
		ExpirationType: redis.HSetEXExpirationEX,
		// TODO: make this configurable
		ExpirationVal: int64((time.Hour * 24).Seconds()),
	}, page.Url, "1")

	// Update crawling queue with new urls
	pipe.ZAddNX(ctx, DomainPendingQueue, *page.DomainQueueMembers...)
	for domain, urls := range *page.DomainAndUrls {
		pipe.LPush(ctx, CrawlQueuePrefix+domain, urls...)
	}

	// Store contents and queue for indexing
	pipe.HSet(ctx, page.Url, hashedPage{
		page.Url,
		string(*page.HtmlContent),
		page.CrawledAt,
		*page.Outlinks,
	})
	pipe.LPush(ctx, IndexPendingQueue, page.Url)

	return rc.retryer.Do(ctx, func() error {
		_, err := pipe.Exec(ctx)
		return err
	}, utils.IsRetryableRedisError)
}
