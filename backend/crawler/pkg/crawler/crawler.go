package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/storage/database"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

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
	crawler.logger.Println("Processing URL:", urlString)

	parsedUrl, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %s: %w", urlString, err)
	}

	if parsedUrl.Scheme == "" {
		parsedUrl.Scheme = "http"
	}

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
						discoveredUrls = append(discoveredUrls, attr.Val)
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

		discoveredUrls, err := crawler.ProcessURL(url)
		if err != nil {
			crawler.logger.Println("Error processing URL:", url, "Error:", err)
			continue
		}

		if err = crawler.updateStorage(url, discoveredUrls); err != nil {
			crawler.logger.Println("Error updating storage for URL:", url, "Error:", err)
			continue
		}
		crawler.redisClient.ZRem(context.Background(), "crawl:processing", url)
		crawler.queueDiscoveredUrls(discoveredUrls)
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

func (crawler *Crawler) updateStorage(urlString string, discoveredUrls []string) error {
	ctx := context.Background()

	tx, err := crawler.dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	queries := &database.Queries{}
	queriesWithTx := queries.WithTx(tx)

	if err = queriesWithTx.UpdateUrlStatus(ctx, database.UpdateUrlStatusParams{
		Url:         urlString,
		CrawlStatus: database.CrawlStatusCompleted,
	}); err != nil {
		return fmt.Errorf("failed to update URL status: %w", err)
	}

	var discoveredUrlsParams []database.CreateUrlsParams

	for _, discoveredUrl := range discoveredUrls {
		discoveredUrlsParams = append(discoveredUrlsParams, database.CreateUrlsParams{
			Url:         discoveredUrl,
			CrawlStatus: database.CrawlStatusPending,
		})
	}

	if _, err = queriesWithTx.CreateUrls(ctx, discoveredUrlsParams); err != nil {
		return fmt.Errorf("failed to create discovered URLs: %w", err)
	}

	return tx.Commit(ctx)
}
