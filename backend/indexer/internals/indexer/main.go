package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"indexer/internals/queue"
	"indexer/internals/storage/database"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Indexer struct {
	logger      *zap.Logger
	workerCount int
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	ctx         context.Context
}

var (
	errNoJobs = errors.New("no jobs available")
)

func NewIndexer(logger *zap.Logger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool, ctx context.Context) *Indexer {
	return &Indexer{
		logger,
		workerCount,
		redisClient,
		dbPool,
		ctx,
	}
}

func (i *Indexer) Start() {
	var wg sync.WaitGroup

	for range i.workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i.worker()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.tfIdfUpdater()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		// transfer urls in processing queue which are older than 30 minute to pending queue
		for {
			processingJobs, err := i.redisClient.ZRangeByScore(i.ctx, queue.ProcessingQueue, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.FormatInt(time.Now().Add(-time.Minute*30).Unix(), 10),
				Offset: 0,
				Count:  100,
			}).Result()
			if err != nil {
				i.logger.Error("Error fetching processing jobs", zap.Error(err))
				time.Sleep(2 * time.Second)
				continue
			}

			if len(processingJobs) == 0 {
				time.Sleep(2 * time.Second)
				continue
			}

			for _, job := range processingJobs {
				if err := i.requeueJob(job); err != nil {
					i.logger.Error("Error requeuing job", zap.Error(err))
				}
			}

		}

	}()
	wg.Wait()
}

func (i *Indexer) worker() {
	for {
		jobDetailsStr, err := i.getNextJob()
		if err != nil {
			i.logger.Error("Error getting job", zap.Error(err))
			continue
		}

		job := &queue.IndexJob{}
		if err := json.Unmarshal([]byte(jobDetailsStr), job); err != nil {
			i.logger.Error("Error unmarshaling job", zap.Error(err))
			continue
		}

		i.logger.Info("Processing job", zap.Int64("id", job.JobId))

		processedJob, err := i.processJob(job)
		if err != nil && err != io.EOF {
			i.logger.Error("Error processing job", zap.Error(err), zap.String("url", job.Url))
			continue
		}

		postingsList, err := i.createPostingsList(processedJob.cleanTextContent, job.JobId)
		if err != nil {
			i.logger.Error("Error creating postings list", zap.String("url", job.Url), zap.Error(err))
			continue
		}

		if err := i.updateQueuesAndStorage(postingsList, job.Url, job.JobId, processedJob); err != nil {
			i.logger.Error("Error updating queues for job", zap.Error(err), zap.String("url", job.Url))
			continue
		}

		i.logger.Info("Successfully processed job:", zap.Int64("id", job.JobId))
	}
}

func (i *Indexer) tfIdfUpdater() {
	queries := database.New(i.dbPool)
	for {
		time.Sleep(5 * time.Minute)
		i.logger.Info("Starting tf-idf updates")
		err := queries.UpdateTfIdf(i.ctx)
		if err != nil {
			i.logger.Error("Error updating tf-idf:", zap.Error(err))
			continue
		}
		i.logger.Info("tf-idf updates complete")
	}
}

func (i *Indexer) getNextJob() (string, error) {
	result, err := i.redisClient.BRPop(i.ctx, time.Second*20, queue.PendingQueue).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", errNoJobs
		}
		return "", err
	}

	// 1 is job, 0 is queue name
	job := result[1]

	pipe := i.redisClient.Pipeline()
	pipe.ZAdd(i.ctx, queue.ProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: job,
	})

	getCmd := pipe.Get(i.ctx, job)

	_, err = pipe.Exec(i.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get next job: %w", err)
	}

	jobDetailsStr, err := getCmd.Result()
	if err != nil {
		return "", fmt.Errorf("failed to get job details: %w", err)
	}

	return jobDetailsStr, nil
}

func (i *Indexer) updateQueuesAndStorage(postingsList *map[string]*Posting, jobUrl string, jobId int64, processedJob *processedJob) error {
	wordDataBatch := make([]database.BatchInsertWordDataParams, 0)

	for word, posting := range *postingsList {
		positionsBytes, err := posting.Positions.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to marshal positions for word %s: %w", word, err)
		}
		postingSerialized := &database.BatchInsertWordDataParams{
			Word:          word,
			UrlID:         posting.DocId,
			TermFrequency: posting.Tf,
			PositionBits:  positionsBytes,
		}
		wordDataBatch = append(wordDataBatch, *postingSerialized)
	}

	tx, err := i.dbPool.Begin(i.ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(i.ctx)

	queries := database.New(i.dbPool)
	queriesWithTx := queries.WithTx(tx)

	err = queriesWithTx.InsertUrlData(i.ctx, database.InsertUrlDataParams{
		UrlID:       jobId,
		Title:       processedJob.title,
		Description: processedJob.description,
		RawContent:  processedJob.rawTextContent})

	if err != nil {
		return fmt.Errorf("failed to insert URL data: %w", err)
	}

	_, err = queriesWithTx.BatchInsertWordData(i.ctx, wordDataBatch)
	if err != nil {
		return fmt.Errorf("failed to word data: %w", err)
	}

	err = i.redisClient.ZRem(i.ctx, queue.ProcessingQueue, jobUrl).Err()
	if err != nil {
		return fmt.Errorf("failed to queue updates: %w", err)
	}

	return tx.Commit(i.ctx)
}

func (i *Indexer) requeueJob(job string) error {
	pipe := i.redisClient.Pipeline()

	pipe.LPush(i.ctx, queue.PendingQueue, job)
	pipe.ZRem(i.ctx, queue.ProcessingQueue, job)

	_, err := pipe.Exec(i.ctx)
	if err != nil {
		return fmt.Errorf("failed to requeue job %s: %w", job, err)
	}

	return nil
}
