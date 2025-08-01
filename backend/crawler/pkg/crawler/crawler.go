package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/storage/database"
	"encoding/json"
	"errors"
	"fmt"
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
}

func NewCrawler(logger *log.Logger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool) *Crawler {
	return &Crawler{
		logger,
		workerCount,
		redisClient,
		dbPool,
	}
}

func (crawler *Crawler) ProcessURL(urlString string) ([]string, error) {
	resp, err := http.Get(urlString)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch URL %s: %s", urlString, resp.Status)
	}

	var discoveredUrls []string

	tokenizer := html.NewTokenizer(resp.Body)

	for {
		tokenType := tokenizer.Next()

		switch tokenType {
		case html.ErrorToken:
			return discoveredUrls, nil // EOF
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						validatedUrl, err := validateUrl(urlString, attr.Val)
						if err != nil {
							crawler.logger.Println("Invalid URL found:", attr.Val, "Error:", err)
						} else {
							isAllowedResource, err := isUrlOfAllowedResourceType(validatedUrl)
							if err != nil {
								crawler.logger.Println("Error checking if URL should be crawled:", validatedUrl, "Error:", err)
							} else if isAllowedResource {
								discoveredUrls = append(discoveredUrls, validatedUrl)
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
	ctx := context.Background()

	file, err := os.Open(seedPath)
	if err != nil {
		crawler.logger.Println("Failed to seed urls: ", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	crawler.logger.Println("Publishing Seed URLs...")
	for scanner.Scan() {
		url := scanner.Text()
		crawler.logger.Println("Seeding URL:", url)

		pipe.LPush(ctx, "crawl:queue", url)
		pipe.SAdd(ctx, "crawl:seen", url)
	}

	result, err := pipe.Exec(ctx)
	if err != nil {
		crawler.logger.Fatalln("Failed to seed urls:", err)
		return
	} else {
		crawler.logger.Println("Seed URLs published successfully:", result)
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

		discoveredUrls, err := crawler.ProcessURL(url)
		if err != nil {
			crawler.logger.Println("Error processing URL:", url, "Error:", err)
			continue
		}

		robotRulesJson, _ := json.Marshal(robotRules)

		if err = crawler.updateStorage(url, domain, discoveredUrls, robotRulesJson); err != nil {
			crawler.logger.Println("Error updating storage for URL:", url, "Error:", err)
			if err = crawler.requeueUrl(url); err != nil {
				crawler.logger.Println("Error requeuing URL after storage update failure:", url, "Error:", err)
			}
			continue
		}

		crawler.redisClient.ZRem(context.Background(), "crawl:processing", url)
		crawler.redisClient.Set(context.Background(), "robots:"+domain, string(robotRulesJson), time.Hour*4)
		crawler.updateDomainDelay(domain)
		crawler.queueDiscoveredUrls(discoveredUrls)
		crawler.logger.Println("Processed URL:", url)
	}
}

func (crawler *Crawler) getNextUrl() (string, error) {
	ctx := context.Background()
	result, err := crawler.redisClient.BRPop(ctx, time.Second*10, "crawl:queue").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("no URLs in queue")
		}
		return "", fmt.Errorf("error fetching URL from queue: %v", err)
	}

	crawler.redisClient.ZAdd(ctx, "crawl:processing", redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: result[1],
	})

	// 1 is url, 0 is queue name
	return result[1], nil
}

func (crawler *Crawler) queueDiscoveredUrls(urls []string) {
	if len(urls) == 0 {
		return
	}
	pipe := crawler.redisClient.Pipeline()
	ctx := context.Background()

	for _, url := range urls {
		exists := pipe.SIsMember(ctx, "crawl:seen", url)
		if exists.Val() {
			continue
		}

		pipe.LPush(ctx, "crawl:queue", url)
		pipe.SAdd(ctx, "crawl:seen", url)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		crawler.logger.Println("Failed to handle discovered URLs:", err)
	}

}

func (crawler *Crawler) updateStorage(urlString, domainString string, discoveredUrls []string, rulesJson []byte) error {
	ctx := context.Background()

	tx, err := crawler.dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	queries := &database.Queries{}
	queriesWithTx := queries.WithTx(tx)

	if err = queriesWithTx.UpdateUrlStatus(ctx, database.UpdateUrlStatusParams{
		Url: urlString,
		FetchedAt: pgtype.Timestamp{
			Time:  time.Now(),
			Valid: true,
		},
	}); err != nil {
		return fmt.Errorf("failed to update URL status: %w", err)
	}

	if err = queriesWithTx.CreateRobotRules(ctx, database.CreateRobotRulesParams{
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

	for _, discoveredUrl := range discoveredUrls {
		discoveredUrlsParams = append(discoveredUrlsParams, database.CreateUrlsParams{
			Url: discoveredUrl,
		})
	}

	if _, err = queriesWithTx.CreateUrls(ctx, discoveredUrlsParams); err != nil {
		return fmt.Errorf("failed to create discovered URLs: %w", err)
	}

	return tx.Commit(ctx)
}

func (crawler *Crawler) requeueUrl(urlString string) error {
	ctx := context.Background()

	if err := crawler.redisClient.LPush(ctx, "crawl:queue", urlString).Err(); err != nil {
		return fmt.Errorf("failed to requeue URL %s: %w", urlString, err)
	}

	if err := crawler.redisClient.ZRem(ctx, "crawl:processing", urlString).Err(); err != nil {
		return fmt.Errorf("failed to remove URL from processing queue %s: %w", urlString, err)
	}

	return nil
}

func (crawler *Crawler) updateDomainDelay(domainString string) error {
	ctx := context.Background()

	if err := crawler.redisClient.HSet(ctx, "crawl:domain_delays", domainString, time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("failed to update domain delay for %s: %w", domainString, err)
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

	robotRules, err := fetchRobotRulesFromWeb(domainString)
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
