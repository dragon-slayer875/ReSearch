package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/queue"
	"crawler/pkg/storage/database"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Crawler struct {
	logger      *log.Logger
	workerCount int
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
	httpClient  *http.Client
	ctx         context.Context
}

var (
	errNotStale = fmt.Errorf("url not stale")
)

func NewCrawler(logger *log.Logger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool, httpClient *http.Client, ctx context.Context) *Crawler {
	return &Crawler{
		logger,
		workerCount,
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
		workerID := i + 1

		go func(id int) {
			defer wg.Done()
			crawler.worker(id)
		}(workerID)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		// transfer urls in processing queue which are older than 30 minute to pending queue
		for {
			time.Sleep(30 * time.Minute)

			processingJobsJson, err := crawler.redisClient.ZRangeByScore(crawler.ctx, queue.ProcessingQueue, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.FormatInt(time.Now().Add(-time.Minute*30).Unix(), 10),
				Offset: 0,
				Count:  100,
			}).Result()
			if err != nil {
				crawler.logger.Println("Error fetching processing jobs:", err)
				time.Sleep(20 * time.Second)
				continue
			}

			processingJobs := make([]redis.Z, 0)
			for _, jobJson := range processingJobsJson {
				var job redis.Z
				if err := json.Unmarshal([]byte(jobJson), &job); err != nil {
					crawler.logger.Println("Error unmarshalling job for recovery:", err)
				}
				processingJobs = append(processingJobs, job)
			}

			if err := crawler.requeueJobs(processingJobs...); err != nil {
				crawler.logger.Println("Error requeuing jobs:", processingJobs, "Error:", err)
			}

			crawler.logger.Println("recovered jobs:", processingJobs)
		}

	}()

	wg.Wait()
}

func (crawler *Crawler) PublishSeedUrls(seedPath string) {
	pipe := crawler.redisClient.Pipeline()

	file, err := os.Open(seedPath)
	if err != nil {
		crawler.logger.Println("Failed to seed urls: ", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		url := scanner.Text()
		normalizedURL, err := normalizeURL(url)
		if err != nil {
			crawler.logger.Println(err)
			continue
		}
		pipe.ZIncrBy(crawler.ctx, queue.PendingQueue, 1000000000000, normalizedURL)
	}

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		crawler.logger.Println("Failed to seed urls:", err)
		return
	}

}

func (crawler *Crawler) worker(workerID int) {
	for {
		job, jobJson, err := crawler.getNextJob()
		if err != nil {
			switch err {
			case redis.Nil:
				crawler.logger.Println("Queue empty")
			case errNotStale:
			default:
				crawler.logger.Println("Error getting URL from queue:", err)
			}
			continue
		}

		url := job.Member.(string)

		domain, err := extractDomainFromUrl(url)
		if err != nil {
			crawler.logger.Println("Error extracting domain from URL:", url, "Error:", err)
			continue
		}

		robotRules, robotRulesStale, err := crawler.GetRobotRules(domain, workerID)
		if err != nil {
			crawler.logger.Println("Error getting robot rules for", url, ". Error:", err)
			if errors.Is(err, rulesLockedError) {
				continue
			}
		}

		if !robotRules.isPolite(domain, crawler.redisClient) {
			if err = crawler.requeueJobs(job); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			if err = crawler.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		if !robotRules.isAllowed(url) {
			if err := crawler.discardJob(jobJson); err != nil {
				crawler.logger.Println("Error discarding URL:", url, "Error:", err)
			}
			if err = crawler.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		crawler.logger.Println("Processing job:", job.Member)

		links, htmlContent, err := crawler.ProcessURL(url)
		if err != nil {
			if err == errNotEnglishPage || err == errNotValidResource {
				if err := crawler.discardJob(jobJson); err != nil {
					crawler.logger.Println("Error discarding URL:", url, "Error:", err)
				}
			} else {
				crawler.logger.Println("Error processing URL:", url, "Error:", err)
			}
			if err = crawler.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		robotRulesJson, _ := json.Marshal(robotRules)

		if err := crawler.updateQueuesAndStorage(url, domain, jobJson, &htmlContent, links, &robotRulesJson, robotRulesStale); err != nil {
			crawler.logger.Println(err)
			if err = crawler.removeRobotRulesFromCache(domain, robotRulesStale); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		crawler.logger.Println("Processed job:", job.Member)
	}
}

func (crawler *Crawler) discardJob(jobJson string) error {
	if err := crawler.redisClient.ZRem(crawler.ctx, queue.ProcessingQueue, jobJson).Err(); err != nil {
		return fmt.Errorf("failed to remove URL from processing queue: %w", err)
	}
	return nil
}

func (crawler *Crawler) requeueJobs(jobs ...redis.Z) error {
	pipe := crawler.redisClient.Pipeline()

	if len(jobs) == 0 {
		return nil
	}

	pipe.ZAdd(crawler.ctx, queue.PendingQueue, jobs...)

	for _, job := range jobs {
		jobJson, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %w", err)
		}
		pipe.ZRem(crawler.ctx, queue.ProcessingQueue, jobJson)
	}

	_, err := pipe.Exec(crawler.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (crawler *Crawler) updateQueuesAndStorage(url, domain string, jobJson string, htmlContent *[]byte, links *[]string, rulesJson *[]byte, robotRulesStale bool) error {
	if err := crawler.redisClient.HSet(crawler.ctx, "crawl:domain_delays", domain, time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("failed to update domain delay for %s: %w", domain, err)
	}

	id, err := crawler.updateStorage(url, domain, links, rulesJson, robotRulesStale)
	if err != nil {
		return fmt.Errorf("failed to update storage for URL %s: %w", url, err)
	}

	if err := crawler.queueForIndexing(htmlContent, url, id); err != nil {
		return fmt.Errorf("failed to queue URL %s for indexing: %w", url, err)
	}

	if err := crawler.redisClient.ZRem(crawler.ctx, queue.ProcessingQueue, jobJson).Err(); err != nil {
		return fmt.Errorf("failed to remove URL %s from processing queue: %w", url, err)
	}

	return nil
}

func (crawler *Crawler) getNextJob() (redis.Z, string, error) {
	result, err := crawler.redisClient.BZPopMax(crawler.ctx, 30*time.Second, queue.PendingQueue).Result()
	if err != nil {
		return redis.Z{}, "", err
	}

	job := result.Z
	url := job.Member.(string)

	stale, err := crawler.isStale(url)
	if err != nil {
		return redis.Z{}, "", err
	}
	if !stale {
		return redis.Z{}, "", errNotStale
	}

	jobJson, err := json.Marshal(job)
	if err != nil {
		return redis.Z{}, "", fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := crawler.redisClient.ZAdd(crawler.ctx, queue.ProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: jobJson,
	}).Err(); err != nil {
		return redis.Z{}, "", err
	}

	return job, string(jobJson), nil
}

func (crawler *Crawler) queueForIndexing(htmlContent *[]byte, url string, id int64) error {
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

	pipe := crawler.redisClient.Pipeline()
	pipe.Set(crawler.ctx, url, payloadJSON, 0)
	pipe.LPush(crawler.ctx, queue.IndexPendingQueue, url)

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline err: %w", err)
	}

	return nil
}

func (crawler *Crawler) updateStorage(url, domain string, links *[]string, rulesJson *[]byte, robotRulesStale bool) (int64, error) {
	tx, err := crawler.dbPool.Begin(crawler.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(crawler.ctx)

	queries := &database.Queries{}
	queriesWithTx := queries.WithTx(tx)

	crawledUrlId, err := queriesWithTx.InsertCrawledUrl(crawler.ctx, url)
	if err != nil {
		return 0, fmt.Errorf("failed to insert URL into postgres: %w", err)
	}

	insertLinksBatch := make([]database.BatchInsertLinksParams, 0)
	var batchErr error
	urlInsertResults := queriesWithTx.InsertUrls(crawler.ctx, *links)
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

	linkInsertResults := queriesWithTx.BatchInsertLinks(crawler.ctx, insertLinksBatch)
	linkInsertResults.Exec(func(i int, err error) {
		batchErr = err
	})

	if batchErr != nil {
		return 0, fmt.Errorf("failed to insert link into postgres: %w", batchErr)
	}

	if robotRulesStale {
		if err = queriesWithTx.CreateRobotRules(crawler.ctx, database.CreateRobotRulesParams{
			Domain:    domain,
			RulesJson: *rulesJson,
		}); err != nil {
			return 0, fmt.Errorf("failed to create robot rules: %w", err)
		}
	}

	return crawledUrlId, tx.Commit(crawler.ctx)
}

func (crawler *Crawler) GetRobotRules(domainString string, workerID int) (*RobotRules, bool, error) {
	var robotRulesStale bool
	var accErr RobotRulesError
	pool := goredis.NewPool(crawler.redisClient)
	rs := redsync.New(pool)

	cacheKey := "robots:" + domainString
	lockKey := "robots_lock:" + domainString

	robotRulesJson, err := crawler.redisClient.Get(crawler.ctx, cacheKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		accErr.CacheError = fmt.Errorf("cache get error: %w", err)
	}

	if robotRules := parseFromCache(robotRulesJson, &accErr); robotRules != nil {
		return robotRules, robotRulesStale, conditionalError(&accErr)
	}

	mutex := rs.NewMutex(lockKey)
	if err := mutex.Lock(); err != nil {
		return nil, false, err
	}

	robotRules, robotRulesStale := crawler.tryGetFromDB(domainString, &accErr)
	if robotRules == nil || robotRulesStale {
		robotRules = crawler.tryGetFromWeb(domainString, &accErr)
		if robotRules == nil {
			robotRules = defaultRobotRules()
		}
	}

	crawler.tryCacheRules(domainString, robotRules, &accErr)

	if ok, err := mutex.Unlock(); !ok || err != nil {
		return robotRules, false, err
	}

	return robotRules, robotRulesStale, conditionalError(&accErr)
}

func (crawler *Crawler) tryGetFromDB(domainString string, accErr *RobotRulesError) (*RobotRules, bool) {
	stale := true
	queries := database.New(crawler.dbPool)
	robotRulesQuery, err := queries.GetRobotRules(crawler.ctx, domainString)
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

func (crawler *Crawler) tryGetFromWeb(domainString string, accErr *RobotRulesError) *RobotRules {
	robotRules, err := fetchRobotRulesFromWeb(domainString, crawler.httpClient)
	if err != nil {
		accErr.WebError = fmt.Errorf("web fetch: %w", err)
		return nil
	}
	return robotRules
}

func (crawler *Crawler) tryCacheRules(domainString string, robotRules *RobotRules, accErr *RobotRulesError) {
	robotRulesJson, err := json.Marshal(robotRules)
	if err != nil {
		accErr.CacheError = fmt.Errorf("marshal for cache: %w", err)
		return
	}

	if err := crawler.redisClient.Set(crawler.ctx, "robots:"+domainString, robotRulesJson, time.Hour*24).Err(); err != nil {
		accErr.CacheError = fmt.Errorf("cache set: %w", err)
	}
}

func (crawler *Crawler) removeRobotRulesFromCache(domain string, robotRulesStale bool) error {
	if robotRulesStale {
		if err := crawler.redisClient.Del(crawler.ctx, "robots:"+domain).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (crawler *Crawler) isStale(url string) (bool, error) {
	stale := true

	queries := database.New(crawler.dbPool)
	urlData, err := queries.GetUrl(crawler.ctx, url)
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
