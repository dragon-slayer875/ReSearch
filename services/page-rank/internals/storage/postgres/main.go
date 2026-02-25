package postgres

import (
	"context"
	"page-rank/internals/retry"
	"page-rank/internals/storage/database"
	"page-rank/internals/utils"

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

func (pc *Client) UpdatePageRank(ctx context.Context, arg *[]database.UpdatePageRankParams) error {
	return pc.retryer.Do(ctx, func() error {
		var tryErr error
		batchResults := pc.queries.UpdatePageRank(ctx, *arg)
		batchResults.Exec(func(i int, err error) {
			if err != nil {
				tryErr = err
				closeErr := batchResults.Close()
				if closeErr != nil {
					tryErr = closeErr
				}
			}
		})

		return tryErr
	}, utils.IsRetryablePostgresError)
}

func (pc *Client) GetLinks(ctx context.Context) (*[]database.GetLinksRow, error) {
	var results []database.GetLinksRow

	err := pc.retryer.Do(ctx, func() error {
		var tryErr error

		results, tryErr = pc.queries.GetLinks(ctx)
		return tryErr
	}, utils.IsRetryablePostgresError)

	return &results, err
}
