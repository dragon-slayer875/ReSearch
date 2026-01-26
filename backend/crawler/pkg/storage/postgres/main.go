package postgres

import (
	"context"
	"crawler/pkg/retry"
	"crawler/pkg/storage/database"
	"crawler/pkg/utils"

	"github.com/jackc/pgx/v5"
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

		return tryErr
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

func (pc *Client) GetUrl(ctx context.Context, url string) (database.Url, error) {
	var urlData database.Url

	err := pc.retryer.Do(ctx, func() error {
		var tryErr error
		urlData, tryErr = pc.Queries.GetUrl(ctx, url)

		return tryErr
	}, utils.IsRetryablePostgresError)

	return urlData, err
}

func (pc *Client) UpdateStorage(ctx context.Context, url string, links *[]string) (crawledUrlId int64, err error) {
	err = pc.retryer.Do(ctx, func() (lastErr error) {
		tx, lastErr := pc.Pool.Begin(ctx)
		if lastErr != nil {
			return lastErr
		}

		defer func() {
			deferErr := tx.Rollback(ctx)
			if deferErr != nil && deferErr != pgx.ErrTxClosed {
				lastErr = deferErr
			}
		}()

		queries := &database.Queries{}
		queriesWithTx := queries.WithTx(tx)

		crawledUrlId, lastErr = queriesWithTx.InsertCrawledUrl(ctx, url)
		if lastErr != nil {
			return lastErr
		}

		insertLinksBatch := make([]database.BatchInsertLinksParams, 0)

		urlInsertResults := queriesWithTx.InsertUrls(ctx, *links)
		urlInsertResults.QueryRow(func(idx int, id int64, rowErr error) {
			if rowErr != nil {
				lastErr = rowErr
				return
			}
			insertLinksBatch = append(insertLinksBatch, database.BatchInsertLinksParams{
				From: crawledUrlId,
				To:   id,
			})
		})
		if lastErr != nil {
			return lastErr
		}

		linkInsertResults := queriesWithTx.BatchInsertLinks(ctx, insertLinksBatch)
		linkInsertResults.Exec(func(i int, rowErr error) {
			if rowErr != nil {
				lastErr = rowErr
			}
		})

		if lastErr != nil {
			return lastErr
		}

		return tx.Commit(ctx)

	}, utils.IsRetryablePostgresError)

	return crawledUrlId, err
}
