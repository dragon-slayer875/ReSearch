package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"indexer/internals/storage/database"
	"indexer/internals/storage/redis"
	"io"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	redisLib "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Indexer struct {
	logger      *zap.Logger
	workerCount int
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	ctx         context.Context
}

type Worker struct {
	logger      *zap.Logger
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	indexerCtx  context.Context
	workerCtx   context.Context
}

func NewIndexer(logger *zap.Logger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool, ctx context.Context) *Indexer {
	return &Indexer{
		logger,
		workerCount,
		redisClient,
		dbPool,
		ctx,
	}
}

func NewWorker(logger *zap.Logger, redisClient *redis.Client, dbPool *pgxpool.Pool, indexerCtx context.Context, workerCtx context.Context) *Worker {
	return &Worker{
		logger,
		redisClient,
		dbPool,
		indexerCtx,
		workerCtx,
	}
}

func (i *Indexer) Start() {
	var wg sync.WaitGroup

	for idx := range i.workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			worker := NewWorker(i.logger.Named(fmt.Sprintf("worker %d", idx)), i.redisClient, i.dbPool, i.ctx, context.Background())
			worker.logger.Debug("Worker initialized")
			worker.work()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		i.tfIdfUpdater()
	}()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	//
	// 	// transfer urls in processing queue which are older than 30 minute to pending queue
	// 	for {
	// 		processingJobs, err := i.redisClient.ZRangeByScore(i.ctx, queue.ProcessingQueue, &redis.ZRangeBy{
	// 			Min:    "0",
	// 			Max:    strconv.FormatInt(time.Now().Add(-time.Minute*30).Unix(), 10),
	// 			Offset: 0,
	// 			Count:  100,
	// 		}).Result()
	// 		if err != nil {
	// 			i.logger.Error("Error fetching processing jobs", zap.Error(err))
	// 			time.Sleep(2 * time.Second)
	// 			continue
	// 		}
	//
	// 		if len(processingJobs) == 0 {
	// 			time.Sleep(2 * time.Second)
	// 			continue
	// 		}
	//
	// 		for _, job := range processingJobs {
	// 			if err := i.requeueJob(job); err != nil {
	// 				i.logger.Error("Error requeuing job", zap.Error(err))
	// 			}
	// 		}
	//
	// 	}
	//
	// }()

	wg.Wait()
}

func (w *Worker) work() {
	for {
		err := w.indexerCtx.Err()
		if err == context.Canceled {
			w.logger.Info("Worker shutting down")
			break
		} else if err != nil {
			w.logger.Fatal("Context error found, indexer shutting down", zap.Error(err))
		}

		url, err := w.getNextUrl()
		if err != nil {
			switch err {
			case redisLib.Nil:
				w.logger.Info("No jobs in queue")
				continue
			default:
				w.logger.Fatal("Failed to get url", zap.Error(err))
			}
		}

		loggerWithoutUrl := w.logger
		w.logger = w.logger.With(zap.String("url", url))

		jobDetailsStr, err := w.getUrlContent(url)
		if err != nil {
			w.logger.Fatal("Failed to get url contents", zap.Error(err))
		}

		job := &redis.IndexJob{}
		if err := json.Unmarshal([]byte(jobDetailsStr), job); err != nil {
			w.logger.Fatal("Failed to unmarshal job", zap.Error(err))
		}

		processedJob, err := w.processJob(job)
		if err != nil && err != io.EOF {
			w.logger.Error("Error processing job", zap.Error(err), zap.String("url", job.Url))
			continue
		}

		postingsList, err := w.createPostingsList(processedJob.cleanTextContent, job.JobId)
		if err != nil {
			w.logger.Error("Error creating postings list", zap.String("url", job.Url), zap.Error(err))
			continue
		}

		if err := w.updateQueuesAndStorage(postingsList, job.Url, job.JobId, processedJob); err != nil {
			w.logger.Error("Error updating queues for job", zap.Error(err), zap.String("url", job.Url))
			continue
		}

		w.logger.Info("Successfully processed job:", zap.Int64("id", job.JobId))

		w.logger = loggerWithoutUrl
	}
}

func (i *Indexer) tfIdfUpdater() {
	queries := database.New(i.dbPool)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-i.ctx.Done():
			i.logger.Info("Tf-idf updater shutting down")
			return

		case <-ticker.C:
			err := queries.UpdateTfIdf(i.ctx)
			if err != nil {
				i.logger.Error("Error updating tf-idf:", zap.Error(err))
				continue
			}
			i.logger.Info("tf-idf updates complete")
		}
	}
}

func (w *Worker) getNextUrl() (string, error) {
	result, err := w.redisClient.BRPop(w.workerCtx, time.Second*20, redis.PendingQueue).Result()
	if err != nil {
		return "", err
	}

	// 1 is url, 0 is queue name
	url := result[1]

	return url, nil
}

func (w *Worker) getUrlContent(url string) (string, error) {
	pipe := w.redisClient.Pipeline()
	pipe.ZAdd(w.workerCtx, redis.ProcessingQueue, redisLib.Z{
		Score:  float64(time.Now().Unix()),
		Member: url,
	})

	getCmd := pipe.Get(w.workerCtx, url)

	_, err := pipe.Exec(w.workerCtx)
	if err != nil {
		return "", err
	}

	return getCmd.Result()
}

func (w *Worker) updateQueuesAndStorage(postingsList *map[string]*Posting, jobUrl string, jobId int64, processedJob *processedJob) error {
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

	tx, err := w.dbPool.Begin(w.workerCtx)
	if err != nil {
		return err
	}

	defer tx.Rollback(w.workerCtx)

	queries := database.New(w.dbPool)
	queriesWithTx := queries.WithTx(tx)

	err = queriesWithTx.InsertUrlData(w.workerCtx, database.InsertUrlDataParams{
		UrlID:       jobId,
		Title:       processedJob.title,
		Description: processedJob.description,
		RawContent:  processedJob.rawTextContent})

	if err != nil {
		return fmt.Errorf("failed to insert URL data: %w", err)
	}

	_, err = queriesWithTx.BatchInsertWordData(w.workerCtx, wordDataBatch)
	if err != nil {
		return fmt.Errorf("failed to word data: %w", err)
	}

	err = w.redisClient.ZRem(w.workerCtx, redis.ProcessingQueue, jobUrl).Err()
	if err != nil {
		return fmt.Errorf("failed to queue updates: %w", err)
	}

	return tx.Commit(w.workerCtx)
}

// func (w *Worker) requeueJob(job string) error {
// 	pipe := w.redisClient.Pipeline()
//
// 	pipe.LPush(w.ctx, queue.PendingQueue, job)
// 	pipe.ZRem(w.ctx, queue.ProcessingQueue, job)
//
// 	_, err := pipe.Exec(w.ctx)
// 	if err != nil {
// 		return fmt.Errorf("failed to requeue job %s: %w", job, err)
// 	}
//
// 	return nil
// }
