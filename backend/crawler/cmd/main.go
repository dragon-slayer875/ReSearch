package main

import (
	"context"
	"crawler/internals/config"
	"crawler/internals/crawler"
	"crawler/internals/retry"
	"crawler/internals/storage/postgres"
	"crawler/internals/storage/redis"
	"crawler/internals/utils"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

type HeaderRoundTripper struct {
	rt      http.RoundTripper
	headers map[string]string
}

func (h *HeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	for key, value := range h.headers {
		if req.Header.Get(key) == "" {
			req.Header.Set(key, value)
		}
	}
	return h.rt.RoundTrip(req)
}

func main() {
	dev := flag.Bool("dev", false, "Enable development environment behavior")
	restrictedMode := flag.Bool("restricted", false, "Restrict crawler to pages of same domains")
	seedPath := flag.String("seed", "seed.txt", "Path to seed URLs file")
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
		logger.Named("retryer"))
	if err != nil {
		logger.Fatal("Failed to initialize retryer", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	transport := &http.Transport{
		MaxIdleConns:    cfg.Crawler.MaxIdleConns,
		MaxConnsPerHost: cfg.Crawler.MaxConnsPerHost,
		IdleConnTimeout: time.Duration(cfg.Crawler.IdleConnTimeout) * time.Second,
	}

	headers := map[string]string{
		"User-Agent": cfg.Crawler.UserAgent,
	}

	httpClient := &http.Client{
		Transport: &HeaderRoundTripper{
			rt:      transport,
			headers: headers,
		},
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, completing running jobs...")
		cancel()
		// look into this
		<-sigChan
		os.Exit(1)
	}()

	Crawler := crawler.NewCrawler(logger, cfg.Crawler.WorkerCount, redisClient, postgresClient, httpClient, ctx, retryer, *restrictedMode)

	Crawler.PublishSeedUrls(*seedPath)

	logger.Info("Starting...")
	Crawler.Start()
}
