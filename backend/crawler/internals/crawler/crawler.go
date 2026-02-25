package crawler

import (
	"bufio"
	"context"
	"crawler/internals/retry"
	"crawler/internals/storage/database"
	"crawler/internals/storage/postgres"
	"crawler/internals/storage/redis"
	"crawler/internals/utils"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/jackc/pgx/v5"
	redisLib "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	errNotStale = fmt.Errorf("URL is not stale")
)

type Crawler struct {
	logger         *zap.Logger
	workerCount    int
	redisClient    *redis.RedisClient
	postgresClient *postgres.Client
	httpClient     *http.Client
	ctx            context.Context
	retryer        *retry.Retryer
	restrictedMode bool
}

type Worker struct {
	logger         *zap.Logger
	redisClient    *redis.RedisClient
	postgresClient *postgres.Client
	httpClient     *http.Client
	crawlerCtx     context.Context
	workerCtx      context.Context
	retryer        *retry.Retryer
	restrictedMode bool
}

func NewCrawler(logger *zap.Logger, workerCount int, redisClient *redis.RedisClient, postgresClient *postgres.Client, httpClient *http.Client, ctx context.Context, retryer *retry.Retryer, restrictedMode bool) *Crawler {
	return &Crawler{
		logger,
		workerCount,
		redisClient,
		postgresClient,
		httpClient,
		ctx,
		retryer,
		restrictedMode,
	}
}

func NewWorker(logger *zap.Logger, redisClient *redis.RedisClient, postgresClient *postgres.Client, httpClient *http.Client, crawlerCtx context.Context, workerCtx context.Context, retryer *retry.Retryer, restrictedMode bool) *Worker {
	return &Worker{
		logger,
		redisClient,
		postgresClient,
		httpClient,
		crawlerCtx,
		workerCtx,
		retryer,
		restrictedMode,
	}
}

func (crawler *Crawler) Start() {
	var wg sync.WaitGroup

	for i := range crawler.workerCount {
		wg.Add(1)

		go func() {
			defer wg.Done()

			workerLogger := crawler.logger.Named(fmt.Sprintf("worker %d", i))

			retryer, err := retry.New(
				crawler.retryer.MaxRetries,
				crawler.retryer.InitialBackoff.String(),
				crawler.retryer.MaxBackoff.String(),
				crawler.retryer.BackoffMultiplier,
				workerLogger.Named("retryer"))
			if err != nil {
				crawler.logger.Fatal("Failed to initialize retryer for worker", zap.Error(err))
			}

			worker := NewWorker(workerLogger, redis.NewWithClient(crawler.redisClient.Client, retryer), postgres.NewWithPool(crawler.postgresClient.Pool, crawler.postgresClient.Queries, retryer), crawler.httpClient, crawler.ctx, context.Background(), retryer, crawler.restrictedMode)
			worker.logger.Debug("Worker initialized")
			worker.work()
		}()
	}

	wg.Wait()
}

func (crawler *Crawler) PublishSeedUrls(seedPath string) {
	domainAndUrls := make(map[string][]any)
	domainQueueMembers := make([]redisLib.Z, 0)
	pipe := crawler.redisClient.Pipeline()

	file, err := os.Open(seedPath)
	if os.IsNotExist(err) {
		crawler.logger.Debug("Seed file not found")
		return
	} else if err != nil {
		crawler.logger.Fatal("Failed to seed urls", zap.Error(err))
	}

	defer func() {
		if deferErr := file.Close(); deferErr != nil {
			crawler.logger.Error("Failed to close seed file", zap.Error(deferErr))
		}
	}()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		err := crawler.ctx.Err()
		if err == context.Canceled {
			crawler.logger.Info("Stopping publishing of seed urls")
			break
		} else if err != nil {
			crawler.logger.Fatal("Context error found, crawler shutting down", zap.Error(err))
		}

		url := scanner.Text()
		normalizedURL, domain, allowed, err := utils.NormalizeURL("", url)
		if err != nil {
			crawler.logger.Warn("Failed to normalize URL", zap.String("raw_url", url), zap.Error(err))
			continue
		}

		if !allowed {
			continue
		}

		domainQueueMembers = append(domainQueueMembers, redisLib.Z{
			Member: domain,
			Score:  float64(time.Now().Unix()),
		})

		domainAndUrls[domain] = append(domainAndUrls[domain], normalizedURL)

	}

	domainQueueCmd := pipe.ZAddNX(crawler.ctx, redis.DomainPendingQueue, domainQueueMembers...)

	for domain, urls := range domainAndUrls {
		pipe.LPush(crawler.ctx, redis.CrawlQueuePrefix+domain, urls...)
	}

	err = crawler.retryer.Do(crawler.ctx, func() error {
		_, err := pipe.Exec(crawler.ctx)
		return err
	}, utils.IsRetryableRedisError)

	if err != nil {
		crawler.logger.Fatal("Failed to seed URLs", zap.Error(err))
	}

	if domainQueueCmd.Err() != nil {
		crawler.logger.Error("Failed to seed URLs", zap.Error(err))
	}

	crawler.logger.Info("Published seed URLs")
}

func (worker *Worker) work() {
	loggerWithoutDomain := worker.logger
	for {
		err := worker.crawlerCtx.Err()
		if err == context.Canceled {
			worker.logger.Info("Worker shutting down")
			break
		} else if err != nil {
			worker.logger.Fatal("Context error found, worker shutting down", zap.Error(err))
			break
		}

		domain, err := worker.getNextDomain()
		if err != nil {
			switch err {
			case redisLib.Nil:
				worker.logger.Info("No domains in queue")
				continue
			default:
				worker.logger.Fatal("Failed to get domain", zap.Error(err))
			}
		}

		worker.logger = worker.logger.With(zap.String("domain", domain))

		if err := worker.processDomain(domain); err != nil {
			if strings.HasPrefix(err.Error(), "lock already taken") {
				worker.logger.Info("Domain already acquired by another worker")
			} else {
				worker.logger.Fatal("Failed to process domain", zap.Error(err))
			}
		}
		worker.logger = loggerWithoutDomain
	}
}

func (worker *Worker) processDomain(domain string) (err error) {
	domainLockKey := "domain_lock:" + domain

	pool := goredis.NewPool(worker.redisClient.Client)
	rs := redsync.New(pool)

	mutex := rs.NewMutex(domainLockKey)
	redsync.WithExpiry(15 * time.Minute).Apply(mutex)

	if err = mutex.Lock(); err != nil {
		return err
	}

	defer func() {
		if _, unlockErr := mutex.Unlock(); unlockErr != nil {
			err = unlockErr
		}
	}()

	robotRules, err := worker.getRobotRules(domain)
	if err != nil {
		return err
	}

	loggerWithoutUrl := worker.logger

urlLoop:
	for {
		err = worker.crawlerCtx.Err()
		if err != nil {
			if err = worker.redisClient.ZAddNX(worker.workerCtx, redis.DomainPendingQueue, redisLib.Z{
				Member: domain,
				Score:  float64(time.Now().Unix()),
			}).Err(); err != nil {
				return err
			}

			if err == context.Canceled {
				worker.logger.Info("Stopping further processing")
				break
			} else {
				return err
			}
		}

		url, err := worker.getNextUrlForDomain(domain)
		if err != nil {
			switch err {
			case redisLib.Nil:
				worker.logger.Info("Domain's URL queue is empty")
				break urlLoop
			case errNotStale:
				worker.logger.Debug("Freshly crawled, skipping", zap.String("url", url))
				continue
			default:
				worker.logger.Fatal("Failed to get next URL", zap.Error(err), zap.String("url", url))
			}
		}

		worker.logger = worker.logger.With(zap.String("url", url))

		err = worker.processUrl(url, domain, robotRules)
		if err != nil {
			worker.logger.Fatal("Failed to process URL", zap.Error(err))
		}

		_, err = mutex.Extend()
		if err != nil {
			worker.logger.Fatal("Failed to extend mutex expiry", zap.Error(err))
		}

		worker.logger.Debug("Mutex extended", zap.Time("expiry", mutex.Until()))

		worker.logger = loggerWithoutUrl
	}

	return nil
}

func (worker *Worker) processUrl(url, domain string, robotRules *RobotRules) error {
	worker.logger.Info("Processing URL")

	page := new(utils.CrawledPage)
	page.Url = url
	page.Domain = domain

	worker.logger.Info("Checking politeness")
	polite, err := robotRules.isPolite(worker.workerCtx, domain, worker.redisClient)
	if err != nil && err != redisLib.Nil {
		return err
	}
	if !polite {
		worker.logger.Info("Impolite to crawl, sleeping..")
		time.Sleep(time.Second * time.Duration(robotRules.CrawlDelay))
	}

	worker.logger.Info("Crawling")
	err = worker.crawlUrl(page)
	if err != nil {
		switch err {
		case errNotEnglishPage, errNotValidResource:
			worker.logger.Debug("Url leads to unsupported resource type or non english page")
		case errNotOkayHttpCode:
			worker.logger.Info("Url returns http status out of okay range. Add to queue to retry", zap.Int("status_code", page.HttpStatusCode))
		default:
			if utils.IsRetryableNetworkError(err) {
				if !utils.IsInternetAvailable() {
					return fmt.Errorf("no internet. %w", err)
				}
			}
			worker.logger.Warn("Error crawling url. Add to queue to try again", zap.Error(err))
		}

		worker.logger.Info("Discarding")
		if discardErr := worker.redisClient.HSet(worker.workerCtx, redis.DomainDelayHashKey, page.Domain, time.Now().Unix()).Err(); discardErr != nil {
			worker.logger.Fatal("Failed to discard URL", zap.Error(discardErr))
		}

		return nil
	}

	worker.logger.Info("Saving crawled content and updating queues")
	if err := worker.redisClient.UpdateRedis(worker.workerCtx, page); err != nil {
		return err
	}

	worker.logger.Info("Processed URL")
	return nil
}

func (worker *Worker) getNextDomain() (string, error) {
	domainWithScore, err := worker.redisClient.BZPopMin(worker.workerCtx, 15*time.Second, redis.DomainPendingQueue).Result()
	if err != nil {
		return "", err
	}

	return domainWithScore.Member.(string), nil
}

func (worker *Worker) getNextUrlForDomain(domain string) (string, error) {
	result, err := worker.redisClient.BRPop(worker.workerCtx, 15*time.Second, redis.CrawlQueuePrefix+domain).Result()
	if err != nil {
		return "", err
	}

	url := result[1]

	fresh, err := worker.isFresh(url)
	if err != nil {
		return url, err
	}
	if fresh {
		return url, errNotStale
	}

	return url, nil
}

func (worker *Worker) getRobotRules(domain string) (*RobotRules, error) {
	cacheKey := "robots:" + domain

	worker.logger.Debug("Getting robot rules")

	cacheResult, err := worker.redisClient.Get(worker.workerCtx, cacheKey).Result()
	if err != nil && !errors.Is(err, redisLib.Nil) {
		return nil, err
	}

	robotRulesJsonBytes := []byte(cacheResult)

	robotRules := worker.parseRobotRules(robotRulesJsonBytes)
	if robotRules != nil {
		return robotRules, nil
	}

	worker.logger.Debug("Robot rules cache miss, getting from DB")

	robotRulesQuery, err := worker.postgresClient.GetRobotRules(worker.workerCtx, domain)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	robotRules = worker.parseRobotRules(robotRulesQuery.RulesJson)
	if robotRules == nil {
		worker.logger.Debug("Robot rules DB miss, getting robot rules from web")
		robotRules, err = worker.fetchRobotRulesFromWeb(domain)
		if err != nil {
			return nil, err
		}
	}

	if robotRules == nil {
		worker.logger.Debug("Robot rules not found, using default rules")
		robotRules = defaultRobotRules()
	}

	robotRulesJsonBytes, err = json.Marshal(robotRules)
	if err != nil {
		return nil, err
	}

	if err = worker.storeRobotRules(domain, robotRulesJsonBytes); err != nil {
		return nil, err
	}

	worker.logger.Debug("Caching robot rules")

	if err := worker.redisClient.Set(worker.workerCtx, cacheKey, robotRulesJsonBytes, time.Hour*24).Err(); err != nil {
		return nil, err
	}

	return robotRules, nil
}

func (worker *Worker) storeRobotRules(domain string, robotRulesJson []byte) error {
	if err := worker.postgresClient.CreateRobotRules(worker.workerCtx, database.CreateRobotRulesParams{
		Domain:    domain,
		RulesJson: robotRulesJson,
	}); err != nil {
		return err
	}

	return nil
}

func (worker *Worker) isFresh(url string) (bool, error) {
	fresh, err := worker.redisClient.Client.HExists(worker.workerCtx, redis.FreshHashKey, url).Result()
	if err != nil && err != redisLib.Nil {
		return fresh, err
	}
	if err == redisLib.Nil {
		worker.logger.Warn("Staleness check gives nil too")
	}

	return fresh, nil
}
