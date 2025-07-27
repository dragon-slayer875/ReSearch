package main

import (
	"crawler/pkg/config"
	"crawler/pkg/crawler"
	"crawler/pkg/queue"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	configPath := flag.String("config", "config/config.yaml", "Path to configuration file")
	flag.Parse()

	logger := log.New(os.Stdout, "crawler: ", log.LstdFlags|log.Lshortfile)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatalln("Failed to load config:", err)
	}

	err = godotenv.Load()
	if err != nil {
		logger.Fatalln("Error loading environment variables:", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Println("Received shutdown signal, exiting...")
		os.Exit(0)
	}()

	redisClient, err := queue.NewRedisClient(os.Getenv("REDIS_URL"))
	if err != nil {
		logger.Fatalf("Failed to connect to Redis: %v", err)
	}

	Crawler := crawler.NewCrawler(logger, cfg.Crawler.WorkerCount, redisClient)

	if cfg.Crawler.SeedPath != "" {
		Crawler.LoadSeeds(cfg.Crawler.SeedPath)
	}

	logger.Println("Starting...")
	Crawler.Start()
}
