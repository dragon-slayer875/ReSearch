package main

import (
	"context"
	"crawler/pkg/config"
	"crawler/pkg/crawler"
	"crawler/pkg/queue"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	debug := flag.Bool("debug", false, "Enable debug logs")
	seedPath := flag.String("seed", "", "Path to seed URLs file")
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := zap.Must(zap.NewProduction())
	if *debug {
		zapConfig := zap.Config{
			Level:         zap.NewAtomicLevelAt(zap.DebugLevel),
			Development:   true,
			OutputPaths:   []string{"logs.txt"},
			Encoding:      "console",
			EncoderConfig: zap.NewDevelopmentEncoderConfig(),
		}
		logger = zap.Must(zapConfig.Build())
	}

	defer logger.Sync()

	sugaredLogger := logger.Sugar().Named("Crawler")

	ctx := context.Background()

	cfg, err := config.Load(*configPath)
	if err != nil {
		sugaredLogger.Fatalln("Failed to load config:", err)
	}
	sugaredLogger.Debugln("Config loaded")

	err = godotenv.Load(*envPath)
	if err != nil {
		sugaredLogger.Fatalln("Error loading environment variables:", err)
	}
	sugaredLogger.Debugln(".env loaded")

	redisClient, err := queue.NewRedisClient(os.Getenv("REDIS_URL"))
	if err != nil {
		sugaredLogger.Fatalln("Failed to connect to Redis:", err)
	}
	sugaredLogger.Debugln("Redis client initialized")

	dbPool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		sugaredLogger.Fatalln("Failed to connect to PostgreSQL", err)
	}
	sugaredLogger.Debugln("PostgreSQL client initialized")

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

	defer dbPool.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		sugaredLogger.Infoln("Received shutdown signal, exiting...")
		os.Exit(0)
	}()

	Crawler := crawler.NewCrawler(sugaredLogger, cfg.Crawler.WorkerCount, redisClient, dbPool, httpClient, context.Background())

	if *seedPath != "" {
		Crawler.PublishSeedUrls(*seedPath)
	}

	sugaredLogger.Infoln("Starting...")
	Crawler.Start()
}
