package crawler

import (
	"bufio"
	"crawler/pkg/storage/database"
	"log"
	"os"

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
	go func() {
		file, err := os.Open(seedPath)
		if err != nil {
			crawler.logger.Println("Failed to seed urls: ", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			url := scanner.Text()
			crawler.logger.Println("Got link from seed", url)
			//TODO: add the urls to the queues and persistent storages
		}
	}()
}
