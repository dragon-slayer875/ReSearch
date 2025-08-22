package main

import (
	"context"
	"crawler/pkg/config"
	"crawler/pkg/crawler"
	"crawler/pkg/queue"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	seedPath := flag.String("seed", "", "Path to seed URLs file")
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := log.New(os.Stdout, "crawler: ", log.LstdFlags)

	ctx := context.Background()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatalln("Failed to load config:", err)
	}

	err = godotenv.Load(*envPath)
	if err != nil {
		logger.Fatalln("Error loading environment variables:", err)
	}

	redisClient, err := queue.NewRedisClient(os.Getenv("REDIS_URL"))
	if err != nil {
		logger.Fatalf("Failed to connect to Redis: %v", err)
	}

	dbPool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		logger.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:    cfg.Crawler.MaxIdleConns,
			MaxConnsPerHost: cfg.Crawler.MaxConnsPerHost,
			IdleConnTimeout: time.Duration(cfg.Crawler.IdleConnTimeout) * time.Second,
		},
	}

	defer dbPool.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Println("Received shutdown signal, exiting...")
		os.Exit(0)
	}()

	Crawler := crawler.NewCrawler(logger, cfg.Crawler.WorkerCount, redisClient, dbPool, httpClient, context.Background())

	if *seedPath != "" {
		Crawler.PublishSeedUrls(*seedPath)
	}

	logger.Println("Starting...")
	Crawler.Start()
}
