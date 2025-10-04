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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
			processingJobs, err := crawler.redisClient.ZRangeByScore(crawler.ctx, queue.ProcessingQueue, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.FormatInt(time.Now().Add(-time.Minute*30).Unix(), 10),
				Offset: 0,
				Count:  100,
			}).Result()
			if err != nil {
				crawler.logger.Println("Error fetching processing jobs:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if len(processingJobs) == 0 {
				time.Sleep(2 * time.Second)
				continue
			}

			for _, job := range processingJobs {
				if err := crawler.requeueJob(job); err != nil {
					crawler.logger.Println("Error requeuing job:", job, "Error:", err)
				}
			}

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

	var seedUrls []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		url := scanner.Text()
		normalizedURL, err := normalizeURL(url)
		if err != nil {
			crawler.logger.Println(err)
			continue
		}
		seedUrls = append(seedUrls, normalizedURL)
		pipe.SAdd(crawler.ctx, queue.SeenSet, normalizedURL)
	}

	queries := database.New(crawler.dbPool)
	ids, err := queries.BatchInsertUrls(crawler.ctx, database.BatchInsertUrlsParams{
		Column1: seedUrls,
	})
	if err != nil {
		crawler.logger.Println("Failed to seed urls:", err)
		return
	}

	for idx, id := range ids {
		job := queue.Job{Url: seedUrls[idx], Id: id}
		jobJson, _ := json.Marshal(job)
		pipe.LPush(crawler.ctx, queue.PendingQueue, string(jobJson))
	}

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		crawler.logger.Println("Failed to seed urls:", err)
		return
	}

}

func (crawler *Crawler) worker(workerID int) {
	for {
		jobJson, err := crawler.getNextJob()
		if err != nil {
			crawler.logger.Println(err)
			time.Sleep(2 * time.Second)
			continue
		}
		var job queue.Job
		if err := json.Unmarshal([]byte(jobJson), &job); err != nil {
			crawler.logger.Println("Error unmarshalling job:", jobJson, "Error:", err)
			continue
		}
		url := job.Url
		id := job.Id

		domain, err := extractDomainFromUrl(url)
		if err != nil {
			crawler.logger.Println("Error extracting domain from URL:", url, "Error:", err)
			continue
		}

		robotRules, rulesExistInDb, err := crawler.GetRobotRules(domain, workerID)
		if err != nil {
			crawler.logger.Println("Error getting robot rules for", url, ". Error:", err)
			if errors.Is(err, rulesLockedError) {
				continue
			}
		}

		if !robotRules.isPolite(domain, crawler.redisClient) {
			if err = crawler.requeueJob(jobJson); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		if !robotRules.isAllowed(url) {
			if err := crawler.discardUrl(jobJson); err != nil {
				crawler.logger.Println("Error discarding URL:", url, "Error:", err)
			}
			continue
		}

		crawler.logger.Println("Processing job:", jobJson)

		discoveredUrls, htmlContent, err := crawler.ProcessURL(url)
		if err != nil {
			if err == errNotEnglishPage {
				if err := crawler.discardUrl(jobJson); err != nil {
					crawler.logger.Println("Error discarding URL:", url, "Error:", err)
				}
			}
			crawler.logger.Println("Error processing URL:", url, "Error:", err)
			continue
		}

		robotRulesJson, _ := json.Marshal(robotRules)

		if err := crawler.updateQueuesAndStorage(url, domain, jobJson, id, &htmlContent, discoveredUrls, &robotRulesJson, rulesExistInDb); err != nil {
			crawler.logger.Println("Error updating queues and storage for URL:", url, "Error:", err, discoveredUrls)
			continue
		}

		crawler.logger.Println("Processed job:", jobJson)
	}
}

func (crawler *Crawler) discardUrl(jobJson string) error {
	if err := crawler.redisClient.ZRem(crawler.ctx, queue.ProcessingQueue, jobJson).Err(); err != nil {
		return fmt.Errorf("failed to remove URL from processing queue: %w", err)
	}

	crawler.logger.Println("Discarded job:", jobJson)
	return nil
}

func (crawler *Crawler) updateQueuesAndStorage(url, domain, jobJson string, id int64, htmlContent *[]byte, discoveredUrls *[]string, rulesJson *[]byte, rulesExistInDb bool) error {
	if err := crawler.redisClient.HSet(crawler.ctx, "crawl:domain_delays", domain, time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("failed to update domain delay for %s: %w", domain, err)
	}

	discoveredUrlsWithIds, err := crawler.updateStorage(url, domain, discoveredUrls, rulesJson, rulesExistInDb)
	if err != nil {
		return fmt.Errorf("failed to update storage for URL %s: %w", url, err)
	}

	if err := crawler.queueDiscoveredUrls(discoveredUrlsWithIds); err != nil {
		return fmt.Errorf("failed to queue discovered URLs: %w", err)
	}

	if err := crawler.redisClient.ZRem(crawler.ctx, queue.ProcessingQueue, jobJson).Err(); err != nil {
		return fmt.Errorf("failed to remove URL %s from processing queue: %w", url, err)
	}

	if err := crawler.queueForIndexing(htmlContent, url, id); err != nil {
		return fmt.Errorf("failed to queue URL %s for indexing: %w", url, err)
	}

	return nil
}

func (crawler *Crawler) getNextJob() (string, error) {
	result, err := crawler.redisClient.BRPop(crawler.ctx, time.Second*20, queue.PendingQueue).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("no URLs in queue")
		}
		return "", fmt.Errorf("error fetching URL from queue: %v", err)
	}

	// 1 is job, 0 is queue name
	job := result[1]

	crawler.redisClient.ZAdd(crawler.ctx, queue.ProcessingQueue, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: job,
	})

	return job, nil
}

func (crawler *Crawler) queueDiscoveredUrls(urls *map[string]int64) error {
	if len(*urls) == 0 {
		return nil
	}

	pipe := crawler.redisClient.Pipeline()

	for discoveredUrl, id := range *urls {
		job := queue.Job{Url: discoveredUrl, Id: id}
		jobJson, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal discovered URL %s: %w", discoveredUrl, err)
		}
		pipe.LPush(crawler.ctx, queue.PendingQueue, string(jobJson))
	}

	if _, err := pipe.Exec(crawler.ctx); err != nil {
		return err
	}

	return nil
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

func (crawler *Crawler) updateStorage(url, domain string, discoveredUrls *[]string, rulesJson *[]byte, rulesExistInDb bool) (*map[string]int64, error) {
	tx, err := crawler.dbPool.Begin(crawler.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(crawler.ctx)

	queries := &database.Queries{}
	queriesWithTx := queries.WithTx(tx)

	if err = queriesWithTx.UpdateUrlStatus(crawler.ctx, database.UpdateUrlStatusParams{
		Url: url,
		FetchedAt: pgtype.Timestamp{
			Time:  time.Now(),
			Valid: true,
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to update URL status: %w", err)
	}

	if !rulesExistInDb {
		if err = queriesWithTx.CreateRobotRules(crawler.ctx, database.CreateRobotRulesParams{
			Domain:    domain,
			RulesJson: *rulesJson,
			FetchedAt: pgtype.Timestamp{
				Time:  time.Now(),
				Valid: true,
			},
		}); err != nil {
			return nil, fmt.Errorf("failed to create robot rules: %w", err)
		}
	}

	ids, err := queriesWithTx.BatchInsertUrls(crawler.ctx, database.BatchInsertUrlsParams{
		Column1: *discoveredUrls,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create discovered URLs: %w", err)
	}

	discoveredUrlsWithIds := make(map[string]int64, len(*discoveredUrls))
	for idx, discoveredUrl := range *discoveredUrls {
		discoveredUrlsWithIds[discoveredUrl] = ids[idx]
	}

	return &discoveredUrlsWithIds, tx.Commit(crawler.ctx)
}

func (crawler *Crawler) requeueJob(job string) error {
	if err := crawler.redisClient.LPush(crawler.ctx, queue.PendingQueue, job).Err(); err != nil {
		return fmt.Errorf("failed to requeue job %s: %w", job, err)
	}

	if err := crawler.redisClient.ZRem(crawler.ctx, queue.ProcessingQueue, job).Err(); err != nil {
		return fmt.Errorf("failed to remove job from processing queue %s: %w", job, err)
	}

	return nil
}

func (crawler *Crawler) GetRobotRules(domainString string, workerID int) (*RobotRules, bool, error) {
	var accErr RobotRulesError

	cacheKey := "robots:" + domainString
	lockKey := "robots_lock:" + domainString

	pipe := crawler.redisClient.Pipeline()
	cacheGet := pipe.Get(crawler.ctx, cacheKey)
	lockSet := pipe.SetNX(crawler.ctx, lockKey, workerID, robotRulesLockTimeout)

	_, err := pipe.Exec(crawler.ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		accErr.CacheError = fmt.Errorf("redis pipeline: %w", err)
	}

	if robotRules := parseFromCache(cacheGet, &accErr); robotRules != nil {
		crawler.redisClient.Del(crawler.ctx, lockKey)
		return robotRules, true, conditionalError(&accErr)
	}

	lockAcquired, lockErr := lockSet.Result()
	if lockErr != nil {
		accErr.LockError = fmt.Errorf("lock acquire: %w", lockErr)
	} else if !lockAcquired {
		return nil, false, rulesLockedError
	}

	defer func() {
		if err := crawler.redisClient.Del(crawler.ctx, lockKey).Err(); err != nil {
			accErr.LockError = fmt.Errorf("unlock failed: %w", err)
		}
	}()

	if robotRules := crawler.tryGetFromDB(domainString, &accErr); robotRules != nil {
		crawler.tryCacheRules(domainString, robotRules, &accErr)
		return robotRules, false, conditionalError(&accErr)
	}

	robotRules := crawler.tryGetFromWeb(domainString, &accErr)
	if robotRules == nil {
		robotRules = defaultRobotRules()
	}

	crawler.tryCacheRules(domainString, robotRules, &accErr)

	return robotRules, false, conditionalError(&accErr)
}

func (crawler *Crawler) tryGetFromDB(domainString string, accErr *RobotRulesError) *RobotRules {
	queries := database.New(crawler.dbPool)
	robotRulesQuery, err := queries.GetRobotRules(crawler.ctx, domainString)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // Not an error, just not found
	}
	if err != nil {
		accErr.DBError = fmt.Errorf("db query: %w", err)
		return nil
	}

	var robotRules RobotRules
	if err := json.Unmarshal(robotRulesQuery.RulesJson, &robotRules); err != nil {
		accErr.DBError = fmt.Errorf("unmarshal db rules: %w", err)
		return nil
	}

	return &robotRules
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

	if err := crawler.redisClient.Set(crawler.ctx, "robots:"+domainString, robotRulesJson, time.Hour*4).Err(); err != nil {
		accErr.CacheError = fmt.Errorf("cache set: %w", err)
	}
}

func parseFromCache(cmd *redis.StringCmd, accErr *RobotRulesError) *RobotRules {
	robotRulesJson, err := cmd.Result()
	if errors.Is(err, redis.Nil) {
		return nil // Not found, not an error
	}
	if err != nil {
		accErr.CacheError = fmt.Errorf("cache get: %w", err)
		return nil
	}

	var robotRules RobotRules
	if err := json.Unmarshal([]byte(robotRulesJson), &robotRules); err != nil {
		accErr.CacheError = fmt.Errorf("unmarshal cached rules: %w", err)
		return nil
	}

	return &robotRules
}
