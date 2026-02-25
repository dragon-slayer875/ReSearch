package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"tf-idf/internals/config"
	"tf-idf/internals/retry"
	"tf-idf/internals/storage/postgres"
	"tf-idf/internals/storage/redis"
	"tf-idf/internals/utils"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
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

	interval, err := time.ParseDuration(cfg.TfIdf.Interval)
	if err != nil {
		logger.Fatal("Failed to parse interval", zap.Error(err))
	}
	logger.Info("Interval configured", zap.Duration("interval", interval))

	lockDuration, err := time.ParseDuration(cfg.TfIdf.LockDuration)
	if err != nil {
		logger.Fatal("Failed to parse lock duration", zap.Error(err))
	}
	logger.Info("Lock duration configured", zap.Duration("lock duration", interval))

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
		logger.Named("retryer"))
	if err != nil {
		logger.Fatal("Failed to initialize retryer", zap.Error(err))
	}

	redisClient, err := redis.New(ctx, os.Getenv("REDIS_URL"), retryer)
	if err != nil {
		logger.Fatal("Failed to initialize redis client:", zap.Error(err))
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

	logger.Info("Starting...")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			tfIdfKey := "tf-idf"

			pool := goredis.NewPool(redisClient.Client)
			rs := redsync.New(pool)

			mutex := rs.NewMutex(tfIdfKey)
			redsync.WithExpiry(lockDuration).Apply(mutex)

			logger.Info("Acquiring work lock")

			if err := mutex.Lock(); err != nil {
				if strings.HasPrefix(err.Error(), "lock already taken") {
					logger.Info("Tf-Idf is already being processed")
					return
				} else {
					logger.Fatal("Failed to acquire work lock", zap.Error(err))
				}
			}

			logger.Info("Work lock acquired")

			logger.Info("Updating tf-idf")

			err := postgresClient.UpdateTfIdf(ctx)
			if err != nil {
				logger.Fatal("Error updating tf-idf", zap.Error(err))
			}

			logger.Info("tf-idf updates complete")
		}
	}
}
