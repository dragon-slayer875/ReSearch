package crawler

import (
	"bufio"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
)

type Crawler struct {
	logger      *log.Logger
	workerCount int
	redisClient *redis.Client
}

func NewCrawler(logger *log.Logger, workerCount int, redisClient *redis.Client) *Crawler {
	return &Crawler{
		logger,
		workerCount,
		redisClient,
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
