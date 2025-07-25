package crawler

import (
	"log"
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
