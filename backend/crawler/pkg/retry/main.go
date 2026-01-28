package retry

import (
	"context"
	"math"
	"time"

	"go.uber.org/zap"
)

type Retryer struct {
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	logger            *zap.Logger
}

func New(maxRetries int, initialBackoffStr string, maxBackoffStr string, backoffMultiplier float64, logger *zap.Logger) (*Retryer, error) {
	initialBackoff, err := time.ParseDuration(initialBackoffStr)
	if err != nil {
		return nil, err
	}

	maxBackoff, err := time.ParseDuration(maxBackoffStr)
	if err != nil {
		return nil, err
	}

	return &Retryer{
		maxRetries,
		initialBackoff,
		maxBackoff,
		backoffMultiplier,
		logger,
	}, nil
}

func (r *Retryer) Do(ctx context.Context, operation func() error, isRetryable func(error) bool) error {
	var lastErr error

	for attempt := 0; attempt <= r.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := operation()

		if err == nil {
			if attempt > 0 {
				r.logger.Info("Operation succeeded after retry",
					zap.Int("attempts", attempt),
				)
			}
			return nil
		}

		lastErr = err

		if !isRetryable(err) {
			r.logger.Debug("Non-retryable error encountered")
			return err
		}

		if attempt == r.MaxRetries {
			break
		}

		backoff := r.calculateBackoff(attempt)

		r.logger.Warn("Retryable error, backing off",
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", r.MaxRetries),
			zap.Duration("backoff", backoff),
			zap.Error(err),
		)

		if err := r.sleep(ctx, backoff); err != nil {
			return err
		}
	}

	return lastErr
}

func (r *Retryer) calculateBackoff(attempt int) time.Duration {
	backoff := float64(r.InitialBackoff) * math.Pow(r.BackoffMultiplier, float64(attempt))

	if backoff > float64(r.MaxBackoff) {
		backoff = float64(r.MaxBackoff)
	}

	return time.Duration(backoff)
}

func (r *Retryer) sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
