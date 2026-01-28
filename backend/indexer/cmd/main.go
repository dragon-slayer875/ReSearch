package main

import (
	"context"
	"flag"
	"indexer/internals/config"
	"indexer/internals/indexer"
	"indexer/internals/queue"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := log.New(os.Stdout, "indexer: ", log.LstdFlags|log.Lshortfile)

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

	defer dbPool.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Println("Received shutdown signal, exiting...")
		os.Exit(0)
	}()

	indexer := indexer.NewIndexer(logger, cfg.Indexer.WorkerCount, redisClient, dbPool, ctx)

	logger.Println("Starting...")
	indexer.Start()
}
