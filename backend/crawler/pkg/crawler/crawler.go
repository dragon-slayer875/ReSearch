package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/queue"
	"crawler/pkg/retry"
	"crawler/pkg/storage/database"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	errNotStale = fmt.Errorf("URL is not stale")
)

type Crawler struct {
	logger      *zap.SugaredLogger
	workerCount int
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	httpClient  *http.Client
	ctx         context.Context
	retryer     *retry.Retryer
}

type Worker struct {
	logger      *zap.SugaredLogger
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	httpClient  *http.Client
	ctx         context.Context
	retryer     *retry.Retryer
}

func NewCrawler(logger *zap.SugaredLogger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool, httpClient *http.Client, ctx context.Context, retryer *retry.Retryer) *Crawler {
	return &Crawler{
		logger,
		workerCount,
		redisClient,
		dbPool,
		httpClient,
		ctx,
		retryer,
	}
}

func NewWorker(logger *zap.SugaredLogger, redisClient *redis.Client, dbPool *pgxpool.Pool, httpClient *http.Client, ctx context.Context, retryer *retry.Retryer) *Worker {
	return &Worker{
		logger,
		redisClient,
		dbPool,
		httpClient,
		ctx,
		retryer,
	}
}

func (crawler *Crawler) Start() {
	var wg sync.WaitGroup

	for i := range crawler.workerCount {
		wg.Add(1)

		go func() {
			defer wg.Done()

			workerLogger := crawler.logger.Named(fmt.Sprintf("Worker %d", i))

			retryer, err := retry.New(
				crawler.retryer.MaxRetries,
				crawler.retryer.InitialBackoff.String(),
				crawler.retryer.MaxBackoff.String(),
				crawler.retryer.BackoffMultiplier,
				workerLogger.Named("Retryer"))
			if err != nil {
				crawler.logger.Fatalln("Failed to initialize retryer", i, err)
			}

			worker := NewWorker(workerLogger, crawler.redisClient, crawler.dbPool, crawler.httpClient, crawler.ctx, retryer)
			worker.logger.Debugln(fmt.Sprintf("Worker %d initialized", i))
			worker.work()
		}()
	}

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	worker := NewWorker(crawler.logger.Named("Recovery Worker"), crawler.redisClient, crawler.dbPool, crawler.httpClient, crawler.ctx)
	// 	worker.logger.Debugln("Recovery Worker initialized")
	//
	// 	// transfer urls in processing queue which are older than 30 minute to pending queue
	// 	for {
	// 		time.Sleep(30 * time.Minute)
	//
	// 		processingJobsJson, err := worker.redisClient.ZRangeByScore(crawler.ctx, queue.ProcessingQueue, &redis.ZRangeBy{
	// 			Min:    "0",
	// 			Max:    strconv.FormatInt(time.Now().Add(-time.Minute*30).Unix(), 10),
	// 			Offset: 0,
	// 			Count:  100,
	// 		}).Result()
	// 		if err != nil {
	// 			worker.logger.Errorln("Error fetching processing jobs:", err)
	// 			time.Sleep(20 * time.Second)
	// 			continue
	// 		}
	//
	// 		processingJobs := make([]redis.Z, 0)
	// 		for _, jobJson := range processingJobsJson {
	// 			var job redis.Z
	// 			if err := json.Unmarshal([]byte(jobJson), &job); err != nil {
	// 				worker.logger.Errorln("Error unmarshalling job for recovery:", err)
	// 			}
	// 			processingJobs = append(processingJobs, job)
	// 		}
	//
	// 		if err := worker.requeueJobs(processingJobs...); err != nil {
	// 			worker.logger.Errorln("Error requeuing jobs:", processingJobs, "Error:", err)
	// 		}
	//
	// 		worker.logger.Errorln("recovered jobs:", processingJobs)
	// 	}
	//
	// }()

	wg.Wait()
}

func (crawler *Crawler) PublishSeedUrls(seedPath string) {
	pipe := crawler.redisClient.Pipeline()

	file, err := os.Open(seedPath)
	if err != nil {
		crawler.logger.Errorln("Failed to seed urls:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		url := scanner.Text()
		normalizedURL, ext, err := normalizeURL("", url)
		if err != nil {
			crawler.logger.Warnln(err)
			continue
		}

		if ext != "" {
			isAllowed := isUrlOfAllowedResourceType(normalizedURL)
			if !isAllowed {
				continue
			}
		}

		domain, err := extractDomainFromUrl(normalizedURL)
		if err != nil {
			crawler.logger.Errorln(err)
			continue
		}

		pipe.ZAddNX(crawler.ctx, queue.DomainPendingQueue, redis.Z{
			Member: domain,
			Score:  float64(time.Now().Unix()),
		})
		pipe.LPush(crawler.ctx, "crawl_queue:"+domain, normalizedURL)
	}

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		crawler.logger.Errorln("Failed to seed urls:", err)
		return
	}
	crawler.logger.Infoln("Published seed URLs")
}

func (worker *Worker) work() {
	loggerWithoutDomain := worker.logger
	for {
		domain, err := worker.getNextDomain()
		if err != nil {
			switch err {
			case redis.Nil:
				worker.logger.Infoln("No domains in queue")
			default:
				worker.logger.Errorln("Error getting URL from queue:", err)
			}
			continue
		}

		worker.logger = worker.logger.With("domain", domain)
		if err := worker.processDomain(domain); err != nil {
			if strings.HasPrefix(err.Error(), "lock already taken") {
				worker.logger.Infow("Domain already acquired by another worker")
			} else {
				worker.logger.Errorln(err)
			}
		}
		worker.logger = loggerWithoutDomain
	}
}

func (worker *Worker) processDomain(domain string) error {
	domainLockKey := "domain_lock:" + domain

	pool := goredis.NewPool(worker.redisClient)
	rs := redsync.New(pool)

	mutex := rs.NewMutex(domainLockKey)
	redsync.WithExpiry(20 * time.Minute).Apply(mutex)

	if err := mutex.Lock(); err != nil {
		return err
	}

	defer func() {
		if _, err := mutex.Unlock(); err != nil {
			worker.logger.Errorln(err)
		}

		// if err := worker.redisClient.ZRem(worker.ctx, queue.DomainProcessingQueue, domain).Err(); err != nil {
		// 	return err
		// }

	}()

	robotRules, err := worker.getRobotRules(domain)
	if err != nil {
		if robotRules == nil {
			return err
		}
		worker.logger.Warnln(err)
	}

	loggerWithoutUrl := worker.logger
	for {
		url, err := worker.getNextUrlForDomain(domain)
		if err == redis.Nil {
			worker.logger.Infoln("Domain's URL queue is empty")
			break
		}
		if err != nil {
			worker.logger.Errorln("Error getting next URL: ", err)
			continue
		}

		worker.logger = worker.logger.With("URL", url)

		err = worker.processUrl(url, domain, robotRules)
		if err != nil {
			switch err {
			case errNotStale, errNotEnglishPage, errNotValidResource:
				worker.logger.Infoln(err)
				if err := worker.discardJob(url); err != nil {
					worker.logger.Errorln("Error discarding URL:", err)
				}
			default:
				worker.logger.Errorln("Error processing URL:", err)
			}
		}

		worker.logger = loggerWithoutUrl
	}

	return nil
}

func (worker *Worker) processUrl(url, domain string, robotRules *RobotRules) error {
	worker.logger.Infoln("Processing URL")

	stale, err := worker.isStale(url)
	if err != nil {
		return err
	}
	if !stale {
		return errNotStale
	}

	worker.logger.Infoln("Checking politeness")
	polite, err := robotRules.isPolite(domain, worker.redisClient)
	if err != nil && err != redis.Nil {
		return err
	}
	if !polite {
		worker.logger.Infoln("Impolite sleeping..")
		time.Sleep(time.Second * time.Duration(robotRules.CrawlDelay))
	}

	worker.logger.Infoln("Crawling")
	links, htmlContent, err := worker.crawlUrl(url, domain)
	if err != nil {
		return err
	}

	worker.logger.Infoln("Updating storage and queues")
	if err := worker.updateQueuesAndStorage(url, &htmlContent, links); err != nil {
		return err
	}

	worker.logger.Infoln("Processed job")
	return nil
}

func (worker *Worker) discardJob(url string) error {
	worker.logger.Infoln("Discarding job")
	err := worker.redisClient.ZRem(worker.ctx, queue.UrlsProcessingQueue, url).Err()

	if err != nil {
		return err
	}

	return nil
}

// func (worker *Worker) requeueJobs(jobs ...redis.Z) error {
// 	if len(jobs) == 0 {
// 		return nil
// 	}
//
// 	worker.logger.Debugln("requeuing jobs", jobs)
//
// 	pipe := worker.redisClient.Pipeline()
// 	pipe.ZAdd(worker.ctx, queue.DomainPendingQueue, jobs...)
//
// 	for _, job := range jobs {
// 		pipe.ZRem(worker.ctx, queue.UrlsProcessingQueue, job.Member.(string))
// 	}
//
// 	_, err := pipe.Exec(worker.ctx)
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }

func (worker *Worker) updateQueuesAndStorage(url string, htmlContent *[]byte, links *[]string) error {
	id, err := worker.updateStorage(url, links)
	if err != nil {
		return fmt.Errorf("failed to update storage for URL %s: %w", url, err)
	}

	if err := worker.updateQueues(htmlContent, url, id); err != nil {
		return fmt.Errorf("failed to queue URL %s for indexing: %w", url, err)
	}

	return nil
}

func (worker *Worker) getNextDomain() (string, error) {
	domainWithScore, err := worker.redisClient.BZPopMin(worker.ctx, time.Second*30, queue.DomainPendingQueue).Result()
	if err != nil {
		return "", err
	}

	return domainWithScore.Member.(string), nil
}

func (worker *Worker) getNextUrlForDomain(domain string) (string, error) {
	result, err := worker.redisClient.BRPop(worker.ctx, 30*time.Second, "crawl_queue:"+domain).Result()
	if err != nil {
		return "", err
	}

	url := result[1]

	err = worker.redisClient.ZAdd(worker.ctx, queue.UrlsProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: url,
	}).Err()
	if err != nil {
		return url, err
	}

	return url, nil
}

func (worker *Worker) updateQueues(htmlContent *[]byte, url string, id int64) error {
	payload := struct {
		Id          int64  `json:"id"`
		Url         string `json:"url"`
		HtmlContent string `json:"html_content"`
		Timestamp   int64  `json:"timestamp"`
	}{
		id, url, string(*htmlContent), time.Now().Unix(),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal indexing payload: %w", err)
	}

	pipe := worker.redisClient.Pipeline()
	pipe.Set(worker.ctx, url, payloadJSON, 0)
	pipe.LPush(worker.ctx, queue.IndexPendingQueue, url)
	pipe.ZRem(worker.ctx, queue.UrlsProcessingQueue, url)

	_, err = pipe.Exec(worker.ctx)
	if err != nil {
		return fmt.Errorf("failed to update queues: %w", err)
	}

	return nil
}

func (worker *Worker) updateStorage(url string, links *[]string) (int64, error) {
	tx, err := worker.dbPool.Begin(worker.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(worker.ctx)

	queries := &database.Queries{}
	queriesWithTx := queries.WithTx(tx)

	crawledUrlId, err := queriesWithTx.InsertCrawledUrl(worker.ctx, url)
	if err != nil {
		return 0, fmt.Errorf("failed to insert URL into postgres: %w", err)
	}

	insertLinksBatch := make([]database.BatchInsertLinksParams, 0)
	var batchErr error
	urlInsertResults := queriesWithTx.InsertUrls(worker.ctx, *links)
	urlInsertResults.QueryRow(func(idx int, id int64, err error) {
		if err != nil {
			batchErr = err
			return
		}
		insertLinksBatch = append(insertLinksBatch, database.BatchInsertLinksParams{
			From: crawledUrlId,
			To:   id,
		})
	})
	if batchErr != nil {
		return 0, fmt.Errorf("failed to insert URL into postgres: %w", batchErr)
	}

	linkInsertResults := queriesWithTx.BatchInsertLinks(worker.ctx, insertLinksBatch)
	linkInsertResults.Exec(func(i int, err error) {
		batchErr = err
	})

	if batchErr != nil {
		return 0, fmt.Errorf("failed to insert link into postgres: %w", batchErr)
	}

	return crawledUrlId, tx.Commit(worker.ctx)
}

func (worker *Worker) getRobotRules(domain string) (*RobotRules, error) {
	cacheKey := "robots:" + domain

	worker.logger.Debugln("Getting robot rules")

	cacheResult, err := worker.redisClient.Get(worker.ctx, cacheKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	robotRulesJsonBytes := []byte(cacheResult)

	robotRules := worker.parseRobotRules(robotRulesJsonBytes)
	if robotRules != nil {
		return robotRules, nil
	}

	worker.logger.Debugln("Robot rules cache miss, getting from DB")

	queries := database.New(worker.dbPool)

	robotRulesQuery, err := queries.GetRobotRules(worker.ctx, domain)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}

	robotRules = worker.parseRobotRules(robotRulesQuery.RulesJson)
	if robotRules == nil {
		worker.logger.Debugln("Robot rules DB miss, getting robot rules from web")
		robotRules, err = worker.fetchRobotRulesFromWeb(domain)
		if err != nil {
			return nil, err
		}
	}

	if robotRules == nil {
		worker.logger.Debugln("Robot rules not found, using default rules")
		robotRules = defaultRobotRules()
	}

	robotRulesJsonBytes, err = json.Marshal(robotRules)
	if err != nil {
		return nil, err
	}

	if err = worker.storeRobotRules(domain, robotRulesJsonBytes); err != nil {
		return nil, err
	}

	worker.logger.Debugln("Caching robot rules")

	if err := worker.redisClient.Set(worker.ctx, cacheKey, robotRules, time.Hour*24).Err(); err != nil {
		return nil, err
	}

	return robotRules, nil
}

func (worker *Worker) storeRobotRules(domain string, robotRulesJson []byte) error {
	queries := database.New(worker.dbPool)
	if err := queries.CreateRobotRules(worker.ctx, database.CreateRobotRulesParams{
		Domain:    domain,
		RulesJson: robotRulesJson,
	}); err != nil {
		return err
	}

	return nil
}

func (worker *Worker) isStale(url string) (bool, error) {
	stale := true

	queries := database.New(worker.dbPool)
	urlData, err := queries.GetUrl(worker.ctx, url)
	if err != nil && err != pgx.ErrNoRows {
		return stale, err
	}
	if err == nil && time.Since(urlData.FetchedAt.Time) < time.Hour*24 {
		stale = false
	}

	return stale, nil
}
