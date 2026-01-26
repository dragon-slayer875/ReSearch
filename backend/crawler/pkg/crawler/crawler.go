package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/queue"
	"crawler/pkg/retry"
	"crawler/pkg/storage/database"
	"crawler/pkg/storage/postgres"
	"crawler/pkg/utils"
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
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	errNotStale = fmt.Errorf("URL is not stale")
)

type Crawler struct {
	logger         *zap.SugaredLogger
	workerCount    int
	redisClient    *queue.RedisClient
	postgresClient *postgres.Client
	httpClient     *http.Client
	ctx            context.Context
	retryer        *retry.Retryer
}

type Worker struct {
	logger         *zap.SugaredLogger
	redisClient    *queue.RedisClient
	postgresClient *postgres.Client
	httpClient     *http.Client
	crawlerCtx     context.Context
	workerCtx      context.Context
	retryer        *retry.Retryer
}

func NewCrawler(logger *zap.SugaredLogger, workerCount int, redisClient *queue.RedisClient, postgresClient *postgres.Client, httpClient *http.Client, ctx context.Context, retryer *retry.Retryer) *Crawler {
	return &Crawler{
		logger,
		workerCount,
		redisClient,
		postgresClient,
		httpClient,
		ctx,
		retryer,
	}
}

func NewWorker(logger *zap.SugaredLogger, redisClient *queue.RedisClient, postgresClient *postgres.Client, httpClient *http.Client, crawlerCtx context.Context, workerCtx context.Context, retryer *retry.Retryer) *Worker {
	return &Worker{
		logger,
		redisClient,
		postgresClient,
		httpClient,
		crawlerCtx,
		workerCtx,
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

			worker := NewWorker(workerLogger, queue.NewWithClient(crawler.redisClient.Client, retryer), postgres.NewWithPool(crawler.postgresClient.Pool, crawler.postgresClient.Queries, retryer), crawler.httpClient, crawler.ctx, context.Background(), retryer)
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
	defer func() {
		if deferErr := file.Close(); deferErr != nil {
			crawler.logger.Errorln(deferErr)
		}
	}()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		err := crawler.ctx.Err()
		if err == context.Canceled {
			crawler.logger.Infoln("Context canceled, stopping publishing of seed urls")
			break
		} else if err != nil {
			crawler.logger.Fatalln("Context error found, crawler shutting down. Err:", err)
		}

		url := scanner.Text()
		normalizedURL, domain, ext, err := utils.NormalizeURL("", url)
		if err != nil {
			crawler.logger.Warnln(err)
			continue
		}

		if ext != "" {
			isAllowed := utils.IsUrlOfAllowedResourceType(normalizedURL)
			if !isAllowed {
				continue
			}
		}

		pipe.ZAddNX(crawler.ctx, queue.DomainPendingQueue, redis.Z{
			Member: domain,
			Score:  float64(time.Now().Unix()),
		})
		pipe.LPush(crawler.ctx, "crawl_queue:"+domain, normalizedURL)
	}

	err = crawler.retryer.Do(crawler.ctx, func() error {
		_, err := pipe.Exec(crawler.ctx)
		return err
	}, utils.IsRetryableRedisError)

	if err != nil {
		crawler.logger.Fatalln("Failed to seed urls:", err)
	}

	crawler.logger.Infoln("Published seed URLs")
}

func (worker *Worker) work() {
	loggerWithoutDomain := worker.logger
	for {
		err := worker.crawlerCtx.Err()
		if err == context.Canceled {
			worker.logger.Infoln("Worker shutting down")
			break
		} else if err != nil {
			worker.logger.Errorln("Context error found, worker shutting down. Error:", err)
			break
		}

		domain, err := worker.getNextDomain()
		if err != nil {
			switch err {
			case redis.Nil:
				worker.logger.Infoln("No domains in queue")
				continue
			default:
				worker.logger.Fatalln("Error getting domain from queue:", err)
			}
		}

		worker.logger = worker.logger.With("domain", domain)
		if err := worker.processDomain(domain); err != nil {
			if strings.HasPrefix(err.Error(), "lock already taken") {
				worker.logger.Infow("Domain already acquired by another worker")
			} else {
				worker.logger.Fatalln("Error processing domain:", err)
			}
		}
		worker.logger = loggerWithoutDomain
	}
}

func (worker *Worker) processDomain(domain string) (err error) {
	domainLockKey := "domain_lock:" + domain

	pool := goredis.NewPool(worker.redisClient.Client)
	rs := redsync.New(pool)

	mutex := rs.NewMutex(domainLockKey)
	redsync.WithExpiry(15 * time.Minute).Apply(mutex)

	if err = mutex.Lock(); err != nil {
		return err
	}

	defer func() {
		if _, unlockErr := mutex.Unlock(); unlockErr != nil {
			err = unlockErr
		}
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
		err = worker.crawlerCtx.Err()
		if err != nil {
			if err = worker.redisClient.ZAddNX(worker.workerCtx, queue.DomainPendingQueue, redis.Z{
				Member: domain,
				Score:  float64(time.Now().Unix()),
			}).Err(); err != nil {
				return err
			}

			if err == context.Canceled {
				worker.logger.Debugln("Shutdown signal received, stopping further processing")
				break
			} else {
				return err
			}
		}

		url, err := worker.getNextUrlForDomain(domain)
		if err == redis.Nil {
			worker.logger.Infoln("Domain's URL queue is empty")
			break
		}
		if err != nil {
			worker.logger.Fatalln("Error getting next URL: ", err)
		}

		worker.logger = worker.logger.With("url", url)

		_, err = mutex.Extend()
		if err != nil {
			worker.logger.Fatalln("Error extending mutex expiry:", err)
		}

		worker.logger.Debugln("Mutex extended till:", mutex.Until())

		err = worker.processUrl(url, domain, robotRules)
		if err != nil {
			switch err {
			case errNotStale, errNotEnglishPage, errNotValidResource:
				worker.logger.Infoln(err)
				if err := worker.discardJob(url); err != nil {
					worker.logger.Fatalln("Error discarding URL:", err)
				}
			default:
				worker.logger.Fatalln("Error processing URL:", err)
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
	polite, err := robotRules.isPolite(worker.workerCtx, domain, worker.redisClient)
	if err != nil && err != redis.Nil {
		return err
	}
	if !polite {
		worker.logger.Infoln("Impolite to crawl, sleeping..")
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
	return worker.redisClient.ZRem(worker.workerCtx, queue.UrlsProcessingQueue, url).Err()
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
	id, err := worker.postgresClient.UpdateStorage(worker.workerCtx, url, links)
	if err != nil {
		return fmt.Errorf("failed to update storage: %w", err)
	}

	if err := worker.updateQueues(htmlContent, url, id); err != nil {
		return fmt.Errorf("failed to update queues: %w", err)
	}

	return nil
}

func (worker *Worker) getNextDomain() (string, error) {
	domainWithScore, err := worker.redisClient.BZPopMin(worker.workerCtx, 15*time.Second, queue.DomainPendingQueue).Result()
	if err != nil {
		return "", err
	}

	return domainWithScore.Member.(string), nil
}

func (worker *Worker) getNextUrlForDomain(domain string) (string, error) {
	result, err := worker.redisClient.BRPop(worker.workerCtx, 15*time.Second, "crawl_queue:"+domain).Result()
	if err != nil {
		return "", err
	}

	url := result[1]

	err = worker.redisClient.ZAdd(worker.workerCtx, queue.UrlsProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: url,
	}).Err()
	if err != nil {
		return "", err
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
	pipe.Set(worker.workerCtx, url, payloadJSON, 0)
	pipe.LPush(worker.workerCtx, queue.IndexPendingQueue, url)
	pipe.ZRem(worker.workerCtx, queue.UrlsProcessingQueue, url)

	err = worker.retryer.Do(worker.workerCtx, func() error {
		_, err := pipe.Exec(worker.workerCtx)
		return err
	}, utils.IsRetryableRedisError)

	if err != nil {
		return err
	}

	return nil
}

func (worker *Worker) getRobotRules(domain string) (*RobotRules, error) {
	cacheKey := "robots:" + domain

	worker.logger.Debugln("Getting robot rules")

	cacheResult, err := worker.redisClient.Get(worker.workerCtx, cacheKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	robotRulesJsonBytes := []byte(cacheResult)

	robotRules := worker.parseRobotRules(robotRulesJsonBytes)
	if robotRules != nil {
		return robotRules, nil
	}

	worker.logger.Debugln("Robot rules cache miss, getting from DB")

	robotRulesQuery, err := worker.postgresClient.GetRobotRules(worker.workerCtx, domain)
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

	if err := worker.redisClient.Set(worker.workerCtx, cacheKey, robotRulesJsonBytes, time.Hour*24).Err(); err != nil {
		return nil, err
	}

	return robotRules, nil
}

func (worker *Worker) storeRobotRules(domain string, robotRulesJson []byte) error {
	if err := worker.postgresClient.CreateRobotRules(worker.workerCtx, database.CreateRobotRulesParams{
		Domain:    domain,
		RulesJson: robotRulesJson,
	}); err != nil {
		return err
	}

	return nil
}

func (worker *Worker) isStale(url string) (bool, error) {
	stale := true

	urlData, err := worker.postgresClient.GetUrl(worker.workerCtx, url)
	if err != nil && err != pgx.ErrNoRows {
		return stale, err
	}
	if err == nil && time.Since(urlData.FetchedAt.Time) < time.Hour*24 {
		stale = false
	}

	return stale, nil
}
