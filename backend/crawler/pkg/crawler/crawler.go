package crawler

import (
	"bufio"
	"bytes"
	"context"
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

func (crawler *Crawler) ProcessURL(urlString string) (map[string]struct{}, error) {
	resp, err := crawler.httpClient.Get(urlString)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch URL %s: %s", urlString, resp.Status)
	}

	var indexingBuffer bytes.Buffer

	teeReader := io.TeeReader(resp.Body, &indexingBuffer)

	discoveredUrls := make(map[string]struct{})

	tokenizer := html.NewTokenizer(teeReader)

	for {
		tokenType := tokenizer.Next()

		switch tokenType {
		case html.ErrorToken:
			go func() {
				if err := crawler.queueForIndexing(indexingBuffer.Bytes(), urlString); err != nil {
					crawler.logger.Printf("Failed to queue URL for indexing: %s, Error: %v", urlString, err)
				}
			}()

			return discoveredUrls, nil // EOF
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						isSeen, err := crawler.redisClient.SIsMember(crawler.ctx, "crawl:seen", attr.Val).Result()
						if err != nil {
							crawler.logger.Fatalln(fmt.Errorf("failed to check if URL is seen: %w", err))
						}
						if isSeen {
							continue
						}
						validatedUrl, err := validateUrl(urlString, attr.Val)
						if err != nil {
							crawler.logger.Println("Invalid URL found:", attr.Val, "Error:", err)
						} else {
							isAllowedResource, err := isUrlOfAllowedResourceType(validatedUrl)
							if err != nil {
								crawler.logger.Println("Error checking if URL should be crawled:", validatedUrl, "Error:", err)
							} else if isAllowedResource {
								discoveredUrls[validatedUrl] = struct{}{}
							}
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

	for range crawler.workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			crawler.worker()
		}()
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

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		url := scanner.Text()

		pipe.LPush(crawler.ctx, "crawl:pending", url)
		pipe.SAdd(crawler.ctx, "crawl:seen", url)
	}

	_, err = pipe.Exec(crawler.ctx)
	if err != nil {
		crawler.logger.Fatalln("Failed to seed urls:", err)
		return
	}

}

func (crawler *Crawler) worker() {
	for {
		url, err := crawler.getNextUrl()
		if err != nil {
			crawler.logger.Println(err)
			time.Sleep(2 * time.Second)
			continue
		}

		domain, err := extractDomainFromUrl(url)
		if err != nil {
			crawler.logger.Println("Error extracting domain from URL:", url, "Error:", err)
			continue
		}

		robotRules, err := crawler.GetRobotRules(domain)
		if err != nil {
			crawler.logger.Println("Error getting robot rules for", url, ". Error:", err)
		}

		if !robotRules.isPolite(domain, crawler.redisClient) {
			if err = crawler.requeueUrl(url); err != nil {
				crawler.logger.Println("Error requeuing URL:", url, "Error:", err)
			}
			continue
		}

		if !robotRules.isAllowed(url) {
			crawler.logger.Println("URL not allowed by robots.txt:", url)
			continue
		}

		crawler.logger.Println("Processing URL:", url, "with domain:", domain)

		discoveredUrls, err := crawler.ProcessURL(url)
		if err != nil {
			crawler.logger.Println("Error processing URL:", url, "Error:", err)
			continue
		}

		robotRulesJson, _ := json.Marshal(robotRules)

		if err := crawler.updateQueuesAndStorage(url, domain, &discoveredUrls, robotRulesJson); err != nil {
			crawler.logger.Println("Error updating queues and storage for URL:", url, "Error:", err)
			continue
		}

		crawler.logger.Println("Processed URL:", url)
	}
}

func (crawler *Crawler) updateQueuesAndStorage(url, domain string, discoveredUrls *map[string]struct{}, rulesJson []byte) error {
	if err := crawler.redisClient.HSet(crawler.ctx, "crawl:domain_delays", domain, time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("failed to update domain delay for %s: %w", domain, err)
	}

	if err := crawler.redisClient.Set(context.Background(), "robots:"+domain, string(rulesJson), time.Hour*4).Err(); err != nil {
		return fmt.Errorf("failed to cache robot rules for domain %s: %w", domain, err)
	}

	if err := crawler.queueDiscoveredUrls(discoveredUrls); err != nil {
		return fmt.Errorf("failed to queue discovered URLs: %w", err)
	}

	if err := crawler.updateStorage(url, domain, discoveredUrls, rulesJson); err != nil {
		return fmt.Errorf("failed to update storage for URL %s: %w", url, err)
	}

	if err := crawler.redisClient.ZRem(context.Background(), "crawl:processing", url).Err(); err != nil {
		return fmt.Errorf("failed to remove URL %s from processing queue: %w", url, err)
	}

	return nil
}

func (crawler *Crawler) getNextUrl() (string, error) {
	result, err := crawler.redisClient.BRPop(crawler.ctx, time.Second*10, "crawl:pending").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("no URLs in queue")
		}
		return "", fmt.Errorf("error fetching URL from queue: %v", err)
	}

	crawler.redisClient.ZAdd(crawler.ctx, "crawl:processing", redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: result[1],
	})

	// 1 is url, 0 is queue name
	return result[1], nil
}

func (crawler *Crawler) queueDiscoveredUrls(urls *map[string]struct{}) error {
	if len(*urls) == 0 {
		return nil
	}
	pipe := crawler.redisClient.Pipeline()

	for url := range *urls {
		exists := pipe.SIsMember(crawler.ctx, "crawl:seen", url)
		if exists.Val() {
			delete(*urls, url)
			continue
		}

		pipe.LPush(crawler.ctx, "crawl:pending", url)
		pipe.SAdd(crawler.ctx, "crawl:seen", url)
	}

	if _, err := pipe.Exec(crawler.ctx); err != nil {
		return err
	}

	return nil
}

func (crawler *Crawler) queueForIndexing(body []byte, url string) error {
	payload := map[string]any{
		"url":       url,
		"body":      string(body),
		"timestamp": time.Now().Unix(),
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal indexing payload: %w", err)
	}

	return crawler.redisClient.LPush(crawler.ctx, "index:pending", payloadJSON).Err()
}

func (crawler *Crawler) updateStorage(urlString, domainString string, discoveredUrls *map[string]struct{}, rulesJson []byte) error {
	tx, err := crawler.dbPool.Begin(crawler.ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(crawler.ctx)

	queries := &database.Queries{}
	queriesWithTx := queries.WithTx(tx)

	if err = queriesWithTx.UpdateUrlStatus(crawler.ctx, database.UpdateUrlStatusParams{
		Url: urlString,
		FetchedAt: pgtype.Timestamp{
			Time:  time.Now(),
			Valid: true,
		},
	}); err != nil {
		return fmt.Errorf("failed to update URL status: %w", err)
	}

	if err = queriesWithTx.CreateRobotRules(crawler.ctx, database.CreateRobotRulesParams{
		Domain:    domainString,
		RulesJson: rulesJson,
		FetchedAt: pgtype.Timestamp{
			Time:  time.Now(),
			Valid: true,
		},
	}); err != nil {
		return fmt.Errorf("failed to create robot rules: %w", err)
	}

	var discoveredUrlsParams []database.CreateUrlsParams

	for discoveredUrl := range *discoveredUrls {
		discoveredUrlsParams = append(discoveredUrlsParams, database.CreateUrlsParams{
			Url: discoveredUrl,
		})
	}

	if _, err = queriesWithTx.CreateUrls(crawler.ctx, discoveredUrlsParams); err != nil {
		return fmt.Errorf("failed to create discovered URLs: %w", err)
	}

	return tx.Commit(crawler.ctx)
}

func (crawler *Crawler) requeueUrl(urlString string) error {
	if err := crawler.redisClient.LPush(crawler.ctx, "crawl:pending", urlString).Err(); err != nil {
		return fmt.Errorf("failed to requeue URL %s: %w", urlString, err)
	}

	if err := crawler.redisClient.ZRem(crawler.ctx, "crawl:processing", urlString).Err(); err != nil {
		return fmt.Errorf("failed to remove URL from processing queue %s: %w", urlString, err)
	}

	return nil
}

func (crawler *Crawler) GetRobotRules(domainString string) (*RobotRules, error) {
	var lastErr error

	robotRulesJson, err := crawler.redisClient.Get(context.Background(), "robots:"+domainString).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		lastErr = fmt.Errorf("redis error: %w", err)
	} else if robotRulesJson != "" {
		robotRules := &RobotRules{}
		if err := json.Unmarshal([]byte(robotRulesJson), robotRules); err != nil {
			lastErr = fmt.Errorf("redis unmarshal error: %w", err)
		} else {
			return robotRules, nil
		}
	}

	queries := database.New(crawler.dbPool)
	robotRulesQuery, err := queries.GetRobotRules(context.Background(), domainString)
	if err != nil {
		if lastErr != nil {
			lastErr = fmt.Errorf("database error: %w (previous: %v)", err, lastErr)
		} else {
			lastErr = fmt.Errorf("database error: %w", err)
		}
	} else {
		robotRules := &RobotRules{}
		if err := json.Unmarshal(robotRulesQuery.RulesJson, robotRules); err != nil {
			if lastErr != nil {
				lastErr = fmt.Errorf("database unmarshal error: %w (previous: %v)", err, lastErr)
			} else {
				lastErr = fmt.Errorf("database unmarshal error: %w", err)
			}
		} else {
			return robotRules, nil
		}
	}

	robotRules, err := fetchRobotRulesFromWeb(domainString, crawler.httpClient)
	if err != nil {
		if lastErr != nil {
			lastErr = fmt.Errorf("web fetch error: %w (previous: %v)", err, lastErr)
		} else {
			lastErr = fmt.Errorf("web fetch error: %w", err)
		}

		defaultRules := defaultRobotRules()
		return defaultRules, fmt.Errorf("all methods failed for domain %s: %v", domainString, lastErr)
	}

	// Successfully fetched from web
	return robotRules, nil
}
