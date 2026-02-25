package redis

import (
	"context"
	"encoding/json"
	"indexer/internals/retry"
	"indexer/internals/utils"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	PendingQueue           = "index:pending"
	ProcessingQueue        = "index:processing"
	PostingPendingQueue    = "post:pending"
	PostingProcessingQueue = "post:processing"
)

type CrawledPage struct {
	Url         string   `json:"url"`
	HtmlContent string   `json:"html_content"`
	Timestamp   int64    `json:"timestamp"`
	Outlinks    []string `json:"outlinks"`
}

type Client struct {
	Client  *redis.Client
	retryer *retry.Retryer
}

func New(ctx context.Context, redisUrl string, retryer *retry.Retryer) (*Client, error) {
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

	return &Client{
		client,
		retryer,
	}, nil
}

func NewWithClient(client *redis.Client, retryer *retry.Retryer) *Client {
	return &Client{
		client,
		retryer,
	}
}

func (rc *Client) Close() error {
	err := rc.retryer.Do(context.Background(), func() error {
		return rc.Client.Close()
	}, utils.IsRetryableRedisError)

	return err
}

func (rc *Client) ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.ZAddNX(ctx, key, members...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.ZAdd(ctx, key, members...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.ZRem(ctx, key, members...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	var redisCmd *redis.ZWithKeyCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.BZPopMin(ctx, timeout, keys...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	var redisCmd *redis.StringSliceCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.BRPop(ctx, timeout, keys...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) Pipeline() redis.Pipeliner {
	return rc.Client.Pipeline()
}

func (rc *Client) TxPipeline() redis.Pipeliner {
	return rc.Client.TxPipeline()
}

func (rc *Client) Get(ctx context.Context, key string) *redis.StringCmd {
	var redisCmd *redis.StringCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.Get(ctx, key)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	var redisCmd *redis.StatusCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.Set(ctx, key, value, expiration)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) HSet(ctx context.Context, key string, values ...any) *redis.IntCmd {
	var redisCmd *redis.IntCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.HSet(ctx, key, values...)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) HGet(ctx context.Context, key string, field string) *redis.StringCmd {
	var redisCmd *redis.StringCmd

	_ = rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.HGet(ctx, key, field)
		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	return redisCmd
}

func (rc *Client) GetUrlContent(ctx context.Context, url string) (*CrawledPage, error) {
	var redisCmd *redis.StringCmd

	err := rc.retryer.Do(ctx, func() error {
		redisCmd = rc.Client.GetDel(ctx, url)

		return redisCmd.Err()
	}, utils.IsRetryableRedisError)

	if err != nil {
		return nil, err
	}

	page := new(CrawledPage)
	if err := json.Unmarshal([]byte(redisCmd.Val()), page); err != nil {
		return nil, err
	}

	return page, nil
}
