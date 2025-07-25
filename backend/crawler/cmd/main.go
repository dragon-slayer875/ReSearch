package main

import (
	"bufio"
	"crawler/pkg/crawler"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	seedPath := flag.String("seed", "", "Path to seed list")
	flag.Parse()

	logger := log.New(os.Stdout, "crawler: ", log.LstdFlags|log.Lshortfile)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Println("Received shutdown signal, exiting...")
		os.Exit(0)
	}()

	crawlQueue := make(chan string)
	URLCrawler := crawler.NewCrawler(crawlQueue, logger)

	go func() {
		if *seedPath == "" {
			logger.Println("No seed path provided")
			return
		}
		file, err := os.Open(*seedPath)
		if err != nil {
			logger.Println("Failed to seed urls: ", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			url := scanner.Text()
			URLCrawler.CrawlQueue <- url
		}
	}()

	logger.Println("Starting...")
	for {
		URLCrawler.ProcessURL(<-URLCrawler.CrawlQueue)
	}
}
