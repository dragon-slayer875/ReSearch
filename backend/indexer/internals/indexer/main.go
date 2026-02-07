package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"indexer/internals/config"
	"indexer/internals/contentProcessing"
	"indexer/internals/retry"
	"indexer/internals/storage/database"
	"indexer/internals/storage/postgres"
	"indexer/internals/storage/redis"
	"indexer/internals/utils"
	"sync"
	"time"

	redisLib "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Indexer struct {
	logger         *zap.Logger
	workerCount    int
	redisClient    *redis.Client
	postgresClient *postgres.Client
	ctx            context.Context
	retryerCfg     *config.RetryerConfig
}

type Worker struct {
	logger         *zap.Logger
	redisClient    *redis.Client
	postgresClient *postgres.Client
	indexerCtx     context.Context
	workerCtx      context.Context
}

func NewIndexer(logger *zap.Logger, workerCount int, redisClient *redis.Client, postgresClient *postgres.Client, ctx context.Context, retryerCfg *config.RetryerConfig) *Indexer {
	return &Indexer{
		logger,
		workerCount,
		redisClient,
		postgresClient,
		ctx,
		retryerCfg,
	}
}

func NewWorker(logger *zap.Logger, redisClient *redis.Client, postgresClient *postgres.Client, indexerCtx context.Context, workerCtx context.Context) *Worker {
	return &Worker{
		logger,
		redisClient,
		postgresClient,
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

			workerLogger := i.logger.Named(fmt.Sprintf("worker %d", idx))

			retryer, err := retry.New(
				i.retryerCfg.MaxRetries,
				i.retryerCfg.InitialBackoff,
				i.retryerCfg.MaxBackoff,
				i.retryerCfg.BackoffMultiplier,
				workerLogger.Named("retryer"))
			if err != nil {
				i.logger.Fatal("Failed to initialize retryer", zap.Error(err))
			}

			worker := NewWorker(workerLogger, redis.NewWithClient(i.redisClient.Client, retryer), postgres.NewWithPool(i.postgresClient, retryer), i.ctx, context.Background())
			worker.logger.Debug("Worker initialized")
			worker.work()
		}()
	}
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
				w.logger.Info("No urls in queue")
				continue
			default:
				w.logger.Fatal("Failed to get url", zap.Error(err))
			}
		}

		loggerWithoutUrl := w.logger
		w.logger = w.logger.With(zap.String("url", url))

		jobDetailsStr, err := w.redisClient.GetUrlContent(w.workerCtx, url)
		if err != nil {
			w.logger.Fatal("Failed to get url contents. Add url to crawl queue again", zap.Error(err))
		}

		job := &redis.IndexJob{}
		if err := json.Unmarshal([]byte(jobDetailsStr), job); err != nil {
			w.logger.Fatal("Failed to unmarshal job", zap.Error(err))
		}

		w.logger.Info("Processing url")

		processedJob, err := contentProcessing.ProcessWebpage(job)
		if err != nil {
			w.logger.Error("Error processing webpage", zap.Error(err))
			continue
		}

		postingsList := utils.CreatePostingsList(processedJob.CleanTextContent, job.JobId)

		if err := w.updateQueuesAndStorage(postingsList, job.Url, job.JobId, processedJob); err != nil {
			w.logger.Fatal("Error updating queues for job", zap.Error(err), zap.String("url", job.Url))
		}

		w.logger.Info("Successfully processed job:", zap.Int64("id", job.JobId))

		w.logger = loggerWithoutUrl
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

func (w *Worker) updateQueuesAndStorage(postingsList *map[string]*utils.Posting, jobUrl string, jobId int64, processedJob *postgres.ProcessedJob) error {
	wordDataBatch := make([]database.BatchInsertWordDataParams, 0)

	for word, posting := range *postingsList {
		positionsBytes, err := posting.Positions.ToBytes()
		if err != nil {
			w.logger.Warn("Failed to marshal positions for word", zap.String("word", word), zap.Error(err))
		}
		postingSerialized := &database.BatchInsertWordDataParams{
			Word:          word,
			UrlID:         posting.DocId,
			TermFrequency: posting.Tf,
			PositionBits:  positionsBytes,
		}
		wordDataBatch = append(wordDataBatch, *postingSerialized)
	}

	err := w.postgresClient.UpdateStorage(w.workerCtx, jobId, &wordDataBatch, processedJob)
	if err != nil {
		return err
	}

	return w.redisClient.ZRem(w.workerCtx, redis.ProcessingQueue, jobUrl).Err()
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
