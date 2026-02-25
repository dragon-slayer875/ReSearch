package main

import (
	"context"
	"errors"
	"flag"
	"indexer/internals/config"
	"indexer/internals/indexer"
	"indexer/internals/retry"
	"indexer/internals/storage/postgres"
	"indexer/internals/storage/redis"
	"indexer/internals/utils"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	dev := flag.Bool("dev", false, "Enable development environment behavior")
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := zap.Must(zap.NewProduction())
	if *dev {
		logger = zap.Must(zap.NewDevelopment())
	}

	defer func() {
		if deferErr := logger.Sync(); deferErr != nil && !errors.Is(deferErr, syscall.EINVAL) {
			logger.Error("Failed to sync logger", zap.Error(deferErr))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load(*configPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debug("Config file not found")
		} else {
			logger.Fatal("Failed to load config", zap.Error(err))
		}
	}
	logger.Debug("Config loaded")

	logger, err = utils.NewConfiguredLogger(dev, cfg)
	if err != nil {
		logger.Fatal("Failed to configure logger", zap.Error(err))
	}

	err = godotenv.Load(*envPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debug("env file not found")
		} else {
			logger.Fatal("Failed to load env file", zap.Error(err))
		}
	}
	logger.Debug("env variables loaded")

	retryer, err := retry.New(
		cfg.Retryer.MaxRetries,
		cfg.Retryer.InitialBackoff,
		cfg.Retryer.MaxBackoff,
		cfg.Retryer.BackoffMultiplier,
		logger.Named("Retryer"))
	if err != nil {
		logger.Fatal("Failed to initialize retryer", zap.Error(err))
	}

	redisClient, err := redis.New(ctx, os.Getenv("REDIS_URL"), retryer)
	if err != nil {
		logger.Fatal("Failed to initialize redis client", zap.Error(err))
	}
	logger.Debug("Redis client initialized")

	defer func() {
		if deferErr := redisClient.Close(); deferErr != nil {
			logger.Error("Failed to close redis client", zap.Error(deferErr))
		}
	}()

	postgresClient, err := postgres.New(ctx, os.Getenv("POSTGRES_URL"), retryer)
	if err != nil {
		logger.Fatal("Failed to connect to PostgreSQL", zap.Error(err))
	}
	logger.Debug("PostgreSQL client initialized")
	defer postgresClient.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, exiting...")
		cancel()
	}()

	indexer := indexer.NewIndexer(logger, cfg.Indexer.WorkerCount, redisClient, postgresClient, ctx, &cfg.Retryer)

	logger.Info("Starting...")
	indexer.Start()
}
