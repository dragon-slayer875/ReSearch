package crawler

import (
	"bufio"
	"log"
	"os"
)

type Crawler struct {
	CrawlQueue chan string
	logger     *log.Logger
}

func NewCrawler(crawlQueue chan string, logger *log.Logger) *Crawler {
	return &Crawler{
		crawlQueue,
		logger,
	}
}

func (crawler *Crawler) ProcessURL(url string) {
	crawler.logger.Println("Processed url:", url)
}

func (crawler *Crawler) Start() {
	for {
		crawler.ProcessURL(<-crawler.CrawlQueue)
	}
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
