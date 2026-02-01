package postgres

import (
	"context"
	"indexer/internals/retry"
	"indexer/internals/storage/database"
	"indexer/internals/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	pool    *pgxpool.Pool
	queries *database.Queries
	retryer *retry.Retryer
}

type ProcessedJob struct {
	RawTextContent   string
	CleanTextContent []string
	Title            string
	Description      string
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

func NewWithPool(client *Client, retryer *retry.Retryer) *Client {
	return &Client{
		client.pool,
		client.queries,
		retryer,
	}
}

func (pc *Client) Close() {
	pc.pool.Close()
}

func (pc *Client) UpdateStorage(ctx context.Context, jobId int64, wordDataBatch *[]database.BatchInsertWordDataParams, processedJob *ProcessedJob) error {
	err := pc.retryer.Do(ctx, func() (lastErr error) {
		tx, lastErr := pc.pool.Begin(ctx)
		if lastErr != nil {
			return lastErr
		}

		defer func() {
			deferErr := tx.Rollback(ctx)
			if deferErr != nil && deferErr != pgx.ErrTxClosed {
				lastErr = deferErr
			}
		}()

		queriesWithTx := pc.queries.WithTx(tx)

		lastErr = queriesWithTx.InsertUrlData(ctx, database.InsertUrlDataParams{
			UrlID:       jobId,
			Title:       processedJob.Title,
			Description: processedJob.Description,
			RawContent:  processedJob.RawTextContent})

		if lastErr != nil {
			return lastErr
		}

		_, lastErr = queriesWithTx.BatchInsertWordData(ctx, *wordDataBatch)
		if lastErr != nil {
			return lastErr
		}

		return tx.Commit(ctx)
	}, utils.IsRetryablePostgresError)

	return err
}

func (pc *Client) UpdateTfIdf(ctx context.Context) error {
	return pc.retryer.Do(ctx, func() error {
		return pc.queries.UpdateTfIdf(ctx)
	}, utils.IsRetryablePostgresError)
}
