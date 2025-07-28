package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/storage/database"
	"errors"
	"fmt"
	"log"
	"net/http"
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
	queries     *database.Queries
}

func NewCrawler(logger *log.Logger, workerCount int, redisClient *redis.Client, dbPool *pgxpool.Pool) *Crawler {
	queries := database.New(dbPool)

	return &Crawler{
		logger,
		workerCount,
		redisClient,
		queries,
	}
}

func (crawler *Crawler) ProcessURL(url string) ([]string, error) {
	crawler.logger.Println("Processing URL:", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch URL %s: %s", url, resp.Status)
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
			time.Sleep(1 * time.Second)
			continue
		}

		discoveredUrls, err := crawler.ProcessURL(url)
		if err != nil {
			crawler.logger.Println("Error processing URL:", url, "Error:", err)
			continue
		}

		crawler.handleDiscoveredUrls(discoveredUrls)
	}
}

func (crawler *Crawler) getNextUrl() (string, error) {
	result, err := crawler.redisClient.BRPop(context.Background(), time.Second*10, "crawl:queue").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("no URLs in queue")
		}
		return "", fmt.Errorf("error fetching URL from queue: %v", err)
	}

	// 1 is url, 0 is queue name
	return result[1], nil
}

func (crawler *Crawler) handleDiscoveredUrls(urls []string) {
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
