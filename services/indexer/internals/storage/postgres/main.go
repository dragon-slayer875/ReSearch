package postgres

import (
	"context"
	"errors"
	"indexer/internals/retry"
	"indexer/internals/storage/database"
	"indexer/internals/utils"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

func (pc *Client) UpdateStorage(ctx context.Context, wordDataBatch *[]database.BatchInsertWordDataParams, linksBatch *[]database.BatchInsertLinksParams, processedPage *utils.IndexerPage) error {
	linkInsertErrs := make([]error, len(*linksBatch))

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
			Url:            processedPage.Url,
			Title:          processedPage.Title,
			Description:    processedPage.Description,
			ContentSummary: processedPage.TextContentSummary,
			CrawledAt: pgtype.Timestamp{
				Time:  time.Unix(processedPage.Timestamp, 0),
				Valid: true,
			}})

		if lastErr != nil {
			return lastErr
		}

		batchInsertResults := queriesWithTx.BatchInsertWordData(ctx, *wordDataBatch)
		batchInsertResults.Exec(func(i int, err error) {
			if err != nil {
				lastErr = err
				batchInsertResults.Close()
			}
		})
		if lastErr != nil {
			return lastErr
		}

		linksInsertResults := queriesWithTx.BatchInsertLinks(ctx, *linksBatch)
		linksInsertResults.Exec(func(i int, err error) {
			linkInsertErrs = append(linkInsertErrs, err)
		})

		return tx.Commit(ctx)
	}, utils.IsRetryablePostgresError)

	if err != nil {
		return err
	}

	if len(linkInsertErrs) == 0 {
		return nil
	}

	return errors.Join(linkInsertErrs...)
}
