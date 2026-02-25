package postgres

import (
	"context"
	"tf-idf/internals/retry"
	"tf-idf/internals/storage/database"
	"tf-idf/internals/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	pool    *pgxpool.Pool
	queries *database.Queries
	retryer *retry.Retryer
}

func New(ctx context.Context, connString string, retryer *retry.Retryer) (*Client, error) {
	var pool *pgxpool.Pool

	err := retryer.Do(ctx, func() error {
		var tryErr error
		pool, tryErr = pgxpool.New(ctx, connString)

		if tryErr != nil {
			return tryErr
		}

		return pool.Ping(ctx)
	}, utils.IsRetryablePostgresError)

	if err != nil {
		return nil, err
	}

	queries := database.New(pool)

	return &Client{
		pool,
		queries,
		retryer,
	}, nil
}

func (pc *Client) Close() {
	pc.pool.Close()
}

func (pc *Client) UpdateTfIdf(ctx context.Context) error {
	return pc.retryer.Do(ctx, func() error {
		return pc.queries.UpdateTfIdf(ctx)
	}, utils.IsRetryablePostgresError)
}
