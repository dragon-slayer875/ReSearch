package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"page-rank/internals/config"
	"page-rank/internals/retry"
	"page-rank/internals/storage/database"
	"page-rank/internals/storage/postgres"
	"page-rank/internals/storage/redis"
	"page-rank/internals/utils"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/jackc/pgx/v5/pgtype"
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

	interval, err := time.ParseDuration(cfg.PageRank.Interval)
	if err != nil {
		logger.Fatal("Failed to parse interval", zap.Error(err))
	}
	logger.Info("Interval configured", zap.Duration("interval", interval))

	lockDuration, err := time.ParseDuration(cfg.PageRank.LockDuration)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		cancel()
		<-sigChan
		os.Exit(1)
	}()

	logger.Info("Starting...")

	work(ctx, logger, postgresClient, redisClient, lockDuration)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping..")
			return

		case <-ticker.C:
			work(ctx, logger, postgresClient, redisClient, lockDuration)
		}
	}
}

func work(ctx context.Context, logger *zap.Logger, postgresClient *postgres.Client, redisClient *redis.RedisClient, lockDuration time.Duration) {
	logger.Info("Working")

	pageRankKey := "pageRank"

	pool := goredis.NewPool(redisClient.Client)
	rs := redsync.New(pool)

	mutex := rs.NewMutex(pageRankKey)
	redsync.WithExpiry(lockDuration).Apply(mutex)

	logger.Info("Acquiring work lock")

	if err := mutex.Lock(); err != nil {
		if strings.HasPrefix(err.Error(), "lock already taken") {
			logger.Info("Page rank is already being processed")
			return
		} else {
			logger.Fatal("Failed to acquire work lock", zap.Error(err))
		}
	}

	logger.Info("Work lock acquired")

	defer func() {
		if _, unlockErr := mutex.Unlock(); unlockErr != nil {
			logger.Fatal("Failed to release work lock", zap.Error(unlockErr))
		}
		logger.Info("Work lock released")
	}()

	logger.Info("Getting links from database")

	result, err := postgresClient.GetLinks(ctx)
	if err != nil {
		logger.Fatal("Failed to get links", zap.Error(err))
	}

	logger.Info("Links fetched from database")

	totalDiscoveredPages := float64(len(*result))

	pageDataMap := make(map[string]*database.GetLinksRow, int64(totalDiscoveredPages))
	pageRankMap := new(sync.Map)
	sinks := make([]string, 0, int64(totalDiscoveredPages))

	logger.Info("Calculating initial ranks")

	for _, item := range *result {
		pageDataMap[item.Url] = &item
		pageRankMap.Store(item.Url, 1/totalDiscoveredPages)
		if item.OutlinksCount == 0 {
			sinks = append(sinks, item.Url)
		}
	}

	logger.Info("Initial ranks calculated")

	dampingFactorAdditionConstant := (1 - 0.85) / totalDiscoveredPages
	totalExceptAnySink := totalDiscoveredPages - 1

	// todo: replace explicit range looping with comparison between page ranks sum of previous iteration and current iteration
	loops := 10

	var wg sync.WaitGroup

workLoop:
	for iter := range loops {
		select {
		case <-ctx.Done():
			logger.Info("Shutdown requested, stopping iterations")
			break workLoop
		default:
			logger.Info("Iteration started", zap.Int("iteration", iter+1))

			_, err = mutex.Extend()
			if err != nil {
				logger.Fatal("Failed to extend mutex expiry", zap.Error(err))
			}
			logger.Debug("Mutex extended", zap.Time("expiry", mutex.Until()))

			newPageRankMap := new(sync.Map)

			for url, data := range pageDataMap {
				wg.Go(func() {
					var sinkRankSum float64
					var backlinkRankSum float64

					for _, sink := range sinks {
						if url == sink {
							continue
						}
						val, ok := pageRankMap.Load(sink)
						if !ok {
							logger.Fatal("No value for page rank found for sink", zap.String("sink", sink), zap.String("url", url))
						}
						sinkRank := (val).(float64) / totalExceptAnySink
						sinkRankSum += sinkRank
					}

					for _, backlink := range (data.Backlinks).([]any) {
						// means no backlinks found, nil returned as sql query data
						if backlink == nil {
							break
						}

						val, ok := pageRankMap.Load(backlink)
						if !ok {
							logger.Fatal("No value for page rank found for backlink", zap.Any("backlink", backlink), zap.String("url", url))
						}
						backlinkStr := (backlink).(string)
						backlinkRankSum += (val).(float64) / float64((pageDataMap[backlinkStr]).OutlinksCount)
					}

					newPageRankVal := dampingFactorAdditionConstant + (0.85 * (sinkRankSum + backlinkRankSum))
					newPageRankMap.Store(url, newPageRankVal)

					logger.Debug("Processed url", zap.String("url", url), zap.Float64("new rank", newPageRankVal), zap.Int("iteration", iter+1))
				})
			}

			wg.Wait()
			pageRankMap = newPageRankMap

			logger.Info("Iteration complete", zap.Int("remaining", loops-iter-1))
		}
	}

	pageRankUpdates := make([]database.UpdatePageRankParams, 0, int64(totalDiscoveredPages))
	var sum float64

	pageRankMap.Range(func(url, rank any) bool {
		sum += (rank).(float64)
		pageRankUpdates = append(pageRankUpdates, database.UpdatePageRankParams{
			Url: url.(string),
			PageRank: pgtype.Float8{
				Float64: rank.(float64),
				Valid:   true,
			},
		})
		return true
	})

	if ctx.Err() != nil {
		logger.Info("Shutdown before completion, skipping database write")
		return
	}

	logger.Info("Saving processed ranks to database")

	err = postgresClient.UpdatePageRank(ctx, &pageRankUpdates)
	if err != nil {
		logger.Fatal("Failed to store page ranks in database", zap.Error(err))
	}

	logger.Info("Saved processed page ranks to database")

	logger.Info("Page rank processing complete", zap.Float64("sum", sum))
}
