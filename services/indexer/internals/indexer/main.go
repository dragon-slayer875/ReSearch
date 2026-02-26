package indexer

import (
	"context"
	"errors"
	"fmt"
	"indexer/internals/config"
	"indexer/internals/contentProcessing"
	"indexer/internals/retry"
	"indexer/internals/storage/database"
	"indexer/internals/storage/postgres"
	"indexer/internals/storage/redis"
	"indexer/internals/utils"
	"sync"

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
				w.logger.Info("No pages in queue")
				continue
			default:
				w.logger.Fatal("Failed to get page", zap.Error(err))
			}
		}

		loggerWithoutUrl := w.logger
		w.logger = w.logger.With(zap.String("url", url))

		w.processUrl(url)

		w.logger = loggerWithoutUrl
	}
}

func (w *Worker) processUrl(url string) {
	crawledPageContent, err := w.redisClient.GetUrlContent(w.workerCtx, url)
	if err != nil && !errors.Is(err, redisLib.Nil) {
		w.logger.Fatal("Failed to get page contents. Add url to crawl queue again", zap.Error(err))
	} else if errors.Is(err, redisLib.Nil) {
		w.logger.Warn("Content not found for given url. Add url to crawl queue again", zap.Error(err))
		return
	}

	w.logger.Info("Processing page")

	processedPage, err := contentProcessing.ProcessCrawledPage(crawledPageContent)
	if err != nil {
		w.logger.Warn("Error processing webpage. Add to crawl queue again", zap.Error(err))
		return
	}

	w.logger.Info("Creating postings list")

	processedPage.PostingsList = utils.CreatePostingsList(processedPage.CleanTextContent, processedPage.Url)

	w.logger.Info("Saving processed page")

	if err := w.updateQueuesAndStorage(processedPage); err != nil {
		w.logger.Fatal("Error updating queues and storage for job", zap.Error(err))
	}

	w.logger.Info("Successfully indexed page")
}

func (w *Worker) getNextUrl() (string, error) {
	result, err := w.redisClient.BRPop(w.workerCtx, 0, redis.PendingQueue).Result()
	if err != nil {
		return "", err
	}

	// 1 is url, 0 is queue name
	url := result[1]

	return url, nil
}

func (w *Worker) updateQueuesAndStorage(processedPage *utils.IndexerPage) error {
	wordDataBatch := make([]database.BatchInsertWordDataParams, 0, len(*processedPage.PostingsList))
	linksBatch := make([]database.BatchInsertLinksParams, len(*processedPage.Outlinks))

	for word, posting := range *processedPage.PostingsList {
		positionsBytes, err := posting.Positions.ToBytes()
		if err != nil {
			w.logger.Warn("Failed to marshal positions for word", zap.String("word", word), zap.Error(err))
		}
		postingSerialized := database.BatchInsertWordDataParams{
			Word:          word,
			Url:           posting.DocUrl,
			TermFrequency: posting.Tf,
			PositionBits:  positionsBytes,
		}
		wordDataBatch = append(wordDataBatch, postingSerialized)
	}

	for idx, outlink := range *processedPage.Outlinks {
		linksBatch[idx] = database.BatchInsertLinksParams{
			From: processedPage.Url,
			To:   outlink,
		}
	}

	return w.postgresClient.UpdateStorage(w.workerCtx, &wordDataBatch, &linksBatch, processedPage)
}
