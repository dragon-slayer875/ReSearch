package crawler

import (
	"bufio"
	"context"
	"crawler/pkg/storage/database"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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

func (crawler *Crawler) ProcessURL(url string) {
	crawler.logger.Println("Processed url:", url)
}

func (crawler *Crawler) Start() {
}

func (crawler *Crawler) LoadSeeds(seedPath string) {
	crawler.logger.Println("Publishing Seed URLs...")

	urlParamsArr := make([]database.CreateUrlsParams, 0)

	file, err := os.Open(seedPath)
	if err != nil {
		crawler.logger.Println("Failed to seed urls: ", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		url := scanner.Text()
		urlParams := database.CreateUrlsParams{
			ID:  uuid.New().String(),
			Url: url,
		}
		urlParamsArr = append(urlParamsArr, urlParams)
	}

	_, err = crawler.queries.CreateUrls(context.Background(), urlParamsArr)
	if err != nil {
		crawler.logger.Fatalln("Failed to seed urls:", err)
		return
	}
}
