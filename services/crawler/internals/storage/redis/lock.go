package redis

import (
	"context"
	"crawler/internals/retry"
	"crawler/internals/utils"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

type Lock struct {
	mutex   *redsync.Mutex
	retryer *retry.Retryer
}

func NewLock(key string, expiry time.Duration, redisClient *redis.Client, retryer *retry.Retryer) *Lock {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)

	mutex := rs.NewMutex(key)
	redsync.WithExpiry(expiry).Apply(mutex)

	return &Lock{
		mutex,
		retryer,
	}
}

func (l *Lock) Lock(ctx context.Context) error {
	return l.retryer.Do(ctx, func() error {
		return l.mutex.Lock()
	}, utils.IsRetryableRedisError)
}

func (l *Lock) Unlock(ctx context.Context) error {
	return l.retryer.Do(ctx, func() error {
		_, err := l.mutex.Unlock()
		return err
	}, utils.IsRetryableRedisError)
}

func (l *Lock) Extend(ctx context.Context) error {
	return l.retryer.Do(ctx, func() error {
		_, err := l.mutex.Extend()
		return err
	}, utils.IsRetryableRedisError)
}

func (l *Lock) Until() time.Time {
	return l.mutex.Until()
}
