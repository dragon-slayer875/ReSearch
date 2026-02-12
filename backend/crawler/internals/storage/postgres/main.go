package postgres

import (
	"context"
	"crawler/internals/retry"
	"crawler/internals/storage/database"
	"crawler/internals/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	Pool    *pgxpool.Pool
	Queries *database.Queries
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

func NewWithPool(pool *pgxpool.Pool, queries *database.Queries, retryer *retry.Retryer) *Client {
	return &Client{
		pool,
		queries,
		retryer,
	}
}

func (pc *Client) Close() {
	pc.Pool.Close()
}

func (pc *Client) GetRobotRules(ctx context.Context, domain string) (database.RobotRule, error) {
	var robotRulesQuery database.RobotRule

	err := pc.retryer.Do(ctx, func() error {
		var tryErr error
		robotRulesQuery, tryErr = pc.Queries.GetRobotRules(ctx, domain)

		return tryErr
	}, utils.IsRetryablePostgresError)

	return robotRulesQuery, err
}

func (pc *Client) CreateRobotRules(ctx context.Context, arg database.CreateRobotRulesParams) error {
	err := pc.retryer.Do(ctx, func() error {
		return pc.Queries.CreateRobotRules(ctx, arg)
	}, utils.IsRetryablePostgresError)

	return err
}
