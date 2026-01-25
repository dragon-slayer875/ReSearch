package utils

import (
	"context"
	"errors"
	"net"

	"github.com/redis/go-redis/v9"
)

func IsRetryableNetworkError(err error) bool {
	var netErr net.Error

	return errors.As(err, &netErr)
}

func IsRetryableRedisConnectionError(err error) bool {
	if errors.Is(err, redis.Nil) {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if errors.Is(err, redis.ErrPoolTimeout) || errors.Is(err, redis.ErrPoolExhausted) {
		return true
	}

	return IsRetryableNetworkError(err)
}
