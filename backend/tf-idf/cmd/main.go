package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"tf-idf/internals/config"
	"tf-idf/internals/storage/database"
	"tf-idf/internals/utils"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

	dbPool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		logger.Fatal("Failed to connect to PostgreSQL", zap.Error(err))
	}
	logger.Debug("PostgreSQL connection initialized")
	defer dbPool.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, exiting...")
		cancel()
	}()

	logger.Info("Starting...")

	queries := database.New(dbPool)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			logger.Info("Updating tf-idf")

			err := queries.UpdateTfIdf(ctx)
			if err != nil {
				logger.Fatal("Error updating tf-idf", zap.Error(err))
			}

			logger.Info("tf-idf updates complete")
		}
	}
}
