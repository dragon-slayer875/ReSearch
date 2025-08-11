package crawler

import (
	"bufio"
	"bytes"
	"context"
	"crawler/pkg/queue"
	"crawler/pkg/storage/database"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/html"
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

func (crawler *Crawler) ProcessURL(urlString, id string) (map[string]string, []byte, error) {
	resp, err := crawler.httpClient.Get(urlString)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("failed to fetch URL %s: %s", urlString, resp.Status)
	}

	var indexingBuffer bytes.Buffer

	teeReader := io.TeeReader(resp.Body, &indexingBuffer)

	discoveredUrls := make(map[string]string)

	tokenizer := html.NewTokenizer(teeReader)

	for {
		tokenType := tokenizer.Next()

		switch tokenType {
		case html.ErrorToken:

			return discoveredUrls, indexingBuffer.Bytes(), nil // EOF
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						validatedUrl, err := validateUrl(urlString, attr.Val)
						if err != nil {
							crawler.logger.Println("Invalid URL found:", attr.Val, "Error:", err)
							continue
						}
						isAllowedResource, err := isUrlOfAllowedResourceType(validatedUrl)
						if err != nil {
							crawler.logger.Println("Error checking if URL should be crawled:", validatedUrl, "Error:", err)
							continue
						}
						if isAllowedResource {
							added, err := crawler.redisClient.SAdd(crawler.ctx, queue.SeenSet, validatedUrl).Result()
							if err != nil {
								crawler.logger.Println("Failed to check if URL is seen:", err)
							}
							if added == 0 {
								continue
							}
							discoveredUrls[validatedUrl] = uuid.New().String()
						}
						break
					}
				}
			}
		}
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

	wg.Wait()
}

func (crawler *Crawler) PublishSeedUrls(seedPath string) {
	pipe := crawler.redisClient.Pipeline()

	file, err := os.Open(seedPath)
	if err != nil {
		crawler.logger.Println("Failed to seed urls: ", err)
	}
	defer file.Close()

	var seedUrls []database.CreateUrlsParams
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var id pgtype.UUID
		url := scanner.Text()
		idString := uuid.New().String()
		id.Scan(idString)

		seedUrls = append(seedUrls, database.CreateUrlsParams{
			ID:  id,
			Url: url,
		})

		pipe.LPush(crawler.ctx, queue.PendingQueue, idString+url)
		pipe.SAdd(crawler.ctx, queue.SeenSet, url)
	}

	queries := database.New(crawler.dbPool)
	_, err = queries.CreateUrls(crawler.ctx, seedUrls)
	if err != nil {
		crawler.logger.Fatalln("Failed to seed urls:", err)
		return
	}

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		crawler.logger.Fatalln("Failed to seed urls:", err)
		return
	}

}

func (crawler *Crawler) worker(workerID int) {
	for {
		job, err := crawler.getNextJob()
		if err != nil {
			crawler.logger.Println(err)
			time.Sleep(2 * time.Second)
			continue
		}
		id, url := job[:36], job[36:]

		domain, err := extractDomainFromUrl(url)
		if err != nil {
			crawler.logger.Println("Error extracting domain from URL:", url, "Error:", err)
			continue
		}

		robotRules, rulesExistInDb, err := crawler.GetRobotRules(domain, workerID)
		if err != nil {
			crawler.logger.Println("Error getting robot rules for", url, ". Error:", err)
			if err == fmt.Errorf(rulesLockedError) {
				continue
			}
		}

		if !robotRules.isPolite(domain, crawler.redisClient) {
			if err = crawler.requeueJob(job); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		if !robotRules.isAllowed(url) {
			crawler.logger.Println("URL not allowed by robots.txt:", url)
			continue
		}

		crawler.logger.Println("Processing URL:", url)

		discoveredUrls, htmlContent, err := crawler.ProcessURL(url, id)
		if err != nil {
			crawler.logger.Println("Error processing URL:", url, "Error:", err)
			continue
		}

		robotRulesJson, _ := json.Marshal(robotRules)

		if err := crawler.updateQueuesAndStorage(url, id, domain, &htmlContent, &discoveredUrls, &robotRulesJson, rulesExistInDb); err != nil {
			crawler.logger.Println("Error updating queues and storage for URL:", url, "Error:", err, discoveredUrls)
			continue
		}

		crawler.logger.Println("Processed URL:", url)
	}
}

func (crawler *Crawler) updateQueuesAndStorage(url, id, domain string, htmlContent *[]byte, discoveredUrls *map[string]string, rulesJson *[]byte, rulesExistInDb bool) error {
	if err := crawler.redisClient.HSet(crawler.ctx, "crawl:domain_delays", domain, time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("failed to update domain delay for %s: %w", domain, err)
	}

	if err := crawler.redisClient.Set(context.Background(), "robots:"+domain, string(*rulesJson), time.Hour*4).Err(); err != nil {
		return fmt.Errorf("failed to cache robot rules for domain %s: %w", domain, err)
	}

	if err := crawler.queueDiscoveredUrls(discoveredUrls); err != nil {
		return fmt.Errorf("failed to queue discovered URLs: %w", err)
	}

	if err := crawler.updateStorage(url, domain, discoveredUrls, rulesJson, rulesExistInDb); err != nil {
		return fmt.Errorf("failed to update storage for URL %s: %w", url, err)
	}

	if err := crawler.redisClient.ZRem(context.Background(), queue.ProcessingQueue, id+url).Err(); err != nil {
		return fmt.Errorf("failed to remove URL %s from processing queue: %w", url, err)
	}

	if err := crawler.queueForIndexing(htmlContent, url, id); err != nil {
		return fmt.Errorf("failed to queue URL %s for indexing: %w", url, err)
	}

	return nil
}

func (crawler *Crawler) getNextJob() (string, error) {
	result, err := crawler.redisClient.BRPop(crawler.ctx, time.Second*10, queue.PendingQueue).Result()
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

func (crawler *Crawler) queueDiscoveredUrls(urls *map[string]string) error {
	if len(*urls) == 0 {
		return nil
	}

	pipe := crawler.redisClient.Pipeline()

	for url, id := range *urls {
		pipe.LPush(crawler.ctx, queue.PendingQueue, id+url)
	}

	if _, err := pipe.Exec(crawler.ctx); err != nil {
		return err
	}

	return nil
}

func (crawler *Crawler) queueForIndexing(htmlContent *[]byte, url, id string) error {
	payload := struct {
		id          string
		url         string
		htmlContent string
		timestamp   int64
	}{
		id, url, string(*htmlContent), time.Now().Unix(),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal indexing payload: %w", err)
	}

	return crawler.redisClient.LPush(crawler.ctx, queue.IndexPendingQueue, payloadJSON).Err()
}

func (crawler *Crawler) updateStorage(url, domain string, discoveredUrls *map[string]string, rulesJson *[]byte, rulesExistInDb bool) error {
	tx, err := crawler.dbPool.Begin(crawler.ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
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
		return fmt.Errorf("failed to update URL status: %w", err)
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
			return fmt.Errorf("failed to create robot rules: %w", err)
		}
	}

	var discoveredUrlsParams []database.CreateUrlsParams

	for discoveredUrl := range *discoveredUrls {
		var id pgtype.UUID
		err = id.Scan((*discoveredUrls)[discoveredUrl])
		if err != nil {
			return fmt.Errorf("failed to scan UUID: %w", err)
		}
		discoveredUrlsParams = append(discoveredUrlsParams, database.CreateUrlsParams{
			ID:  id,
			Url: discoveredUrl,
		})
	}

	if _, err = queriesWithTx.CreateUrls(crawler.ctx, discoveredUrlsParams); err != nil {
		return fmt.Errorf("failed to create discovered URLs: %w", err)
	}

	return tx.Commit(crawler.ctx)
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
		return nil, false, fmt.Errorf(rulesLockedError)
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
