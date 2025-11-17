package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/queue"
	"crawler/pkg/storage/database"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Crawler struct {
	logger      *zap.SugaredLogger
	workerCount int
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	httpClient  *http.Client
	ctx         context.Context
}

type Worker struct {
	logger      *zap.SugaredLogger
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	httpClient  *http.Client
	ctx         context.Context
}

func NewCrawler(logger *zap.SugaredLogger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool, httpClient *http.Client, ctx context.Context) *Crawler {
	return &Crawler{
		logger,
		workerCount,
		redisClient,
		dbPool,
		httpClient,
		ctx,
	}
}

func NewWorker(logger *zap.SugaredLogger, redisClient *redis.Client, dbPool *pgxpool.Pool, httpClient *http.Client, ctx context.Context) *Worker {
	return &Worker{
		logger,
		redisClient,
		dbPool,
		httpClient,
		ctx,
	}
}

func (crawler *Crawler) Start() {
	var wg sync.WaitGroup

	for i := range crawler.workerCount {
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker := NewWorker(crawler.logger.Named(fmt.Sprintf("Worker %d", i)), crawler.redisClient, crawler.dbPool, crawler.httpClient, crawler.ctx)
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
		crawler.logger.Errorln("Failed to seed urls: ", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		url := scanner.Text()
		normalizedURL, err := normalizeURL(url)
		if err != nil {
			crawler.logger.Errorln(err)
			continue
		}
		pipe.ZIncrBy(crawler.ctx, queue.PendingQueue, 1000000000000, normalizedURL)
		crawler.logger.Debugln("Published seed URL:", url)
	}

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		crawler.logger.Errorln("Failed to seed urls:", err)
		return
	}

}

func (worker *Worker) work() {
	for {
		job, err := worker.getNextJob()
		if err != nil {
			switch err {
			case redis.Nil:
				worker.logger.Infoln("Queue empty")
			default:
				worker.logger.Errorln("Error getting URL from queue:", err)
			}
			continue
		}
		worker.logger.Debugln("Got job", job)

		url := job.Member.(string)

		stale, err := worker.isStale(url)
		if err != nil {
			worker.logger.Errorln(err)
			continue
		}
		if !stale {
			worker.logger.Infof("Not stale, skipping &s", url)
			if err := worker.discardJob(url); err != nil {
				worker.logger.Errorln("Error discarding job:", err)
			}
			continue
		}

		domain, err := extractDomainFromUrl(url)
		if err != nil {
			worker.logger.Errorln("Error extracting domain from URL", url, ":", err)
			continue
		}
		worker.logger.Debugln("Got domain", domain)

		robotRules, robotRulesStale, err := worker.GetRobotRules(domain)
		if err != nil {
			if errors.Is(err, rulesLockedError) {
				continue
			}
			worker.logger.Errorln("Error getting robot rules for", url, ". Error:", err)
		}

		if !robotRules.isPolite(domain, worker.redisClient) {
			if err = worker.requeueJobs(job); err != nil {
				worker.logger.Errorln("Error requeuing URL:", url, "Error:", err)
			}
			if err = worker.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				worker.logger.Errorln("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		if !robotRules.isAllowed(url) {
			if err := worker.discardJob(url); err != nil {
				worker.logger.Errorln("Error discarding URL:", url, "Error:", err)
			}
			if err = worker.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				worker.logger.Errorln("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		worker.logger.Infoln("Processing job:", job.Member)

		links, htmlContent, err := worker.ProcessURL(url, domain)
		if err != nil {
			if err == errNotEnglishPage || err == errNotValidResource {
				if err := worker.discardJob(url); err != nil {
					worker.logger.Errorln("Error discarding URL:", url, "Error:", err)
				}
			} else {
				worker.logger.Errorln("Error processing URL:", url, "Error:", err)
			}
			if err = worker.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				worker.logger.Errorln("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		robotRulesJson, _ := json.Marshal(robotRules)

		if err := worker.updateQueuesAndStorage(url, domain, &htmlContent, links, &robotRulesJson, robotRulesStale); err != nil {
			worker.logger.Errorln(err)
			if err = worker.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				worker.logger.Errorln("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		worker.logger.Infoln("Processed job:", job.Member)
	}
}

func (worker *Worker) discardJob(url string) error {
	worker.logger.Debugln("discarding job", url)
	pipe := worker.redisClient.Pipeline()
	pipe.ZRem(worker.ctx, queue.ProcessingQueue, url)
	pipe.HDel(worker.ctx, queue.PrioritySet, url)

	if _, err := pipe.Exec(worker.ctx); err != nil {
		return err
	}

	return nil
}

func (worker *Worker) requeueJobs(jobs ...redis.Z) error {
	if len(jobs) == 0 {
		return nil
	}

	worker.logger.Debugln("requeuing jobs", jobs)

	pipe := worker.redisClient.Pipeline()
	pipe.ZAdd(worker.ctx, queue.PendingQueue, jobs...)

	for _, job := range jobs {
		pipe.ZRem(worker.ctx, queue.ProcessingQueue, job.Member.(string))
		pipe.HDel(worker.ctx, queue.PrioritySet, job.Member.(string))
	}

	_, err := pipe.Exec(worker.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (worker *Worker) updateQueuesAndStorage(url, domain string, htmlContent *[]byte, links *[]string, rulesJson *[]byte, robotRulesStale bool) error {
	id, err := worker.updateStorage(url, domain, links, rulesJson, robotRulesStale)
	if err != nil {
		return fmt.Errorf("failed to update storage for URL %s: %w", url, err)
	}

	if err := worker.queueForIndexing(htmlContent, url, id); err != nil {
		return fmt.Errorf("failed to queue URL %s for indexing: %w", url, err)
	}

	pipe := worker.redisClient.Pipeline()
	pipe.ZRem(worker.ctx, queue.ProcessingQueue, url)
	pipe.HDel(worker.ctx, queue.PrioritySet, url)

	if _, err := pipe.Exec(worker.ctx); err != nil {
		return err
	}

	return nil
}

func (worker *Worker) getNextJob() (redis.Z, error) {
	result, err := worker.redisClient.BZPopMax(worker.ctx, 30*time.Second, queue.PendingQueue).Result()
	if err != nil {
		return redis.Z{}, err
	}

	job := result.Z
	url := job.Member.(string)

	pipe := worker.redisClient.Pipeline()
	pipe.ZAdd(worker.ctx, queue.ProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: url,
	})
	pipe.HSet(worker.ctx, queue.PrioritySet, url, job.Score)

	if _, err := pipe.Exec(worker.ctx); err != nil {
		return redis.Z{}, err
	}

	return job, nil
}

func (worker *Worker) queueForIndexing(htmlContent *[]byte, url string, id int64) error {
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

	_, err = pipe.Exec(worker.ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline err: %w", err)
	}

	return nil
}

func (worker *Worker) updateStorage(url, domain string, links *[]string, rulesJson *[]byte, robotRulesStale bool) (int64, error) {
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

	if robotRulesStale {
		if err = queriesWithTx.CreateRobotRules(worker.ctx, database.CreateRobotRulesParams{
			Domain:    domain,
			RulesJson: *rulesJson,
		}); err != nil {
			return 0, fmt.Errorf("failed to create robot rules: %w", err)
		}
	}

	return crawledUrlId, tx.Commit(worker.ctx)
}

func (worker *Worker) GetRobotRules(domainString string) (*RobotRules, bool, error) {
	var robotRulesStale bool
	var accErr RobotRulesError
	pool := goredis.NewPool(worker.redisClient)
	rs := redsync.New(pool)

	cacheKey := "robots:" + domainString
	lockKey := "robots_lock:" + domainString

	robotRulesJson, err := worker.redisClient.Get(worker.ctx, cacheKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		accErr.CacheError = fmt.Errorf("cache get error: %w", err)
	}

	if robotRules := parseFromCache(robotRulesJson, &accErr); robotRules != nil {
		return robotRules, robotRulesStale, conditionalError(&accErr)
	}
	worker.logger.Debugln("Robot rules cache miss", domainString)

	mutex := rs.NewMutex(lockKey)
	if err := mutex.Lock(); err != nil {
		return nil, false, err
	}
	worker.logger.Debugln("Robot rules cache key acquired", domainString)

	worker.logger.Debugln("Getting Robot rules from DB", domainString)
	robotRules, robotRulesStale := worker.tryGetFromDB(domainString, &accErr)
	if robotRules == nil || robotRulesStale {
		worker.logger.Debugln("Local miss, Getting Robot rules from web", domainString)
		robotRules = worker.tryGetFromWeb(domainString, &accErr)
		if robotRules == nil {
			robotRules = defaultRobotRules()
		}
	}

	worker.logger.Debugln("Caching robot rules", domainString)
	worker.tryCacheRules(domainString, robotRules, &accErr)

	if ok, err := mutex.Unlock(); !ok || err != nil {
		return robotRules, false, err
	}

	return robotRules, robotRulesStale, conditionalError(&accErr)
}

func (worker *Worker) tryGetFromDB(domainString string, accErr *RobotRulesError) (*RobotRules, bool) {
	stale := true
	queries := database.New(worker.dbPool)
	robotRulesQuery, err := queries.GetRobotRules(worker.ctx, domainString)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, stale // Not an error, just not found
	}
	if err != nil {
		accErr.DBError = fmt.Errorf("db query: %w", err)
		return nil, stale
	}

	if time.Since(robotRulesQuery.FetchedAt.Time) < time.Hour*24 {
		stale = false
	}

	var robotRules RobotRules
	if err := json.Unmarshal(robotRulesQuery.RulesJson, &robotRules); err != nil {
		accErr.DBError = fmt.Errorf("unmarshal db rules: %w", err)
		return nil, stale
	}

	return &robotRules, stale
}

func (worker *Worker) tryGetFromWeb(domainString string, accErr *RobotRulesError) *RobotRules {
	robotRules, err := fetchRobotRulesFromWeb(domainString, worker.httpClient)
	if err != nil {
		accErr.WebError = fmt.Errorf("web fetch: %w", err)
		return nil
	}
	return robotRules
}

func (worker *Worker) tryCacheRules(domainString string, robotRules *RobotRules, accErr *RobotRulesError) {
	robotRulesJson, err := json.Marshal(robotRules)
	if err != nil {
		accErr.CacheError = fmt.Errorf("marshal for cache: %w", err)
		return
	}

	if err := worker.redisClient.Set(worker.ctx, "robots:"+domainString, robotRulesJson, time.Hour*24).Err(); err != nil {
		accErr.CacheError = fmt.Errorf("cache set: %w", err)
	}
}

func (worker *Worker) removeRobotRulesFromCache(domain string, robotRulesStale bool) error {
	if robotRulesStale {
		worker.logger.Debugln("removing robot rules from cache", domain)
		if err := worker.redisClient.Del(worker.ctx, "robots:"+domain).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (worker *Worker) isStale(url string) (bool, error) {
	stale := true

	queries := database.New(worker.dbPool)
	urlData, err := queries.GetUrl(worker.ctx, url)
	if err == nil && time.Since(urlData.FetchedAt.Time) < time.Hour*24 {
		stale = false
	}

	if err != nil && err != pgx.ErrNoRows {
		return stale, err
	}

	return stale, nil
}

func parseFromCache(robotRulesJson string, accErr *RobotRulesError) *RobotRules {
	var robotRules RobotRules
	if robotRulesJson == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(robotRulesJson), &robotRules); err != nil {
		accErr.CacheError = fmt.Errorf("unmarshal cached rules: %w", err)
		return nil
	}

	return &robotRules
}
