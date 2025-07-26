package main

import (
	"crawler/pkg/config"
	"crawler/pkg/crawler"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "config/config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

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

	if cfg.Crawler.SeedPath != "" {
		URLCrawler.LoadSeeds(cfg.Crawler.SeedPath)
	}

	logger.Println("Starting...")
	URLCrawler.Start()
}
