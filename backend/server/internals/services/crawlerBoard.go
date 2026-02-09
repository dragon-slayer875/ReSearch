package services

import (
	"context"
	"net/url"
	"server/internals/storage/redis"
	"server/internals/utils"
	"time"

	redisLib "github.com/redis/go-redis/v9"
)

type CrawlerBoardService struct {
	redisClient *redisLib.Client
}

func NewCrawlerBoardService(client *redisLib.Client) *CrawlerBoardService {
	return &CrawlerBoardService{
		client,
	}
}

func (cb *CrawlerBoardService) GetSubmissions(ctx context.Context, order string, page, limit int) (*[]string, error) {
	var results []string
	var err error

	if order == "asc" {
		results, err = cb.redisClient.ZRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page)).Result()
	} else {
		results, err = cb.redisClient.ZRevRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page)).Result()
	}

	return &results, err
}

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, submissions []string) ([]any, error) {
	sortedSetMembers := make([]redisLib.Z, 0, len(submissions))
	successfulSubmissions := make([]any, 0, len(submissions))

	pipe := cb.redisClient.TxPipeline()

	for _, submission := range submissions {
		normalizedUrl, allowed, _ := utils.NormalizeURL("", submission)
		if !allowed {
			continue
		}

		sortedSetMembers = append(sortedSetMembers, redisLib.Z{
			Member: normalizedUrl,
			Score:  float64(time.Now().Unix()),
		})
		successfulSubmissions = append(successfulSubmissions, normalizedUrl)
	}

	pipe.ZAddNX(ctx, redis.CrawlerboardKey, sortedSetMembers...)
	_, err := pipe.Exec(ctx)

	return successfulSubmissions, err
}

func (cb *CrawlerBoardService) AcceptSubmissions(ctx context.Context, submissions []string) (int, error) {
	var successfulSubs int
	redisCmds := make([]*redisLib.IntCmd, 0, len(submissions))

	pipe := cb.redisClient.TxPipeline()

	for _, submission := range submissions {
		redisCmds = append(redisCmds, pipe.ZRem(ctx, redis.CrawlerboardKey, submission))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return successfulSubs, err
	}

	domainQueueMembers := make([]redisLib.Z, 0, len(submissions))
	domainsAndUrls := make(map[redisLib.Z][]any, len(submissions))

	for idx, cmd := range redisCmds {
		val, err := cmd.Result()
		if err != nil {
			return successfulSubs, err
		}

		if val == 0 {
			continue
		}

		parsedUrl, _ := url.Parse(submissions[idx])

		qMember := redisLib.Z{
			Member: parsedUrl.Hostname(),
			Score:  float64(time.Now().Unix()),
		}
		domainQueueMembers = append(domainQueueMembers, qMember)
		domainsAndUrls[qMember] = append(domainsAndUrls[qMember], submissions[idx])
		successfulSubs++
	}

	acceptPipe := cb.redisClient.TxPipeline()

	acceptPipe.ZAddNX(ctx, redis.DomainPendingQueue, domainQueueMembers...)

	for _, qMember := range domainQueueMembers {
		acceptPipe.LPush(ctx, "crawl_queue:"+qMember.Member.(string), domainsAndUrls[qMember]...)
	}

	_, err = acceptPipe.Exec(ctx)
	return successfulSubs, err
}

func (cb *CrawlerBoardService) RejectSubmissions(ctx context.Context, submissions []string) (int64, error) {
	return cb.redisClient.ZRem(ctx, redis.CrawlerboardKey, utils.ToAnySlice(submissions)...).Result()
}
