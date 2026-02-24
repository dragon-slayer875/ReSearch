package services

import (
	"context"
	"net/url"
	"server/internals/storage/redis"
	"server/internals/utils"
	"time"

	"github.com/gofiber/fiber/v3/log"
	redisLib "github.com/redis/go-redis/v9"
)

const (
	MalformedUrls   = "Unsupported/malformed urls"
	ConflictingUrls = "Urls already exist"
	NotFoundUrls    = "Urls not found"
)

type CrawlerBoardService struct {
	redisClient *redisLib.Client
}

func NewCrawlerBoardService(client *redisLib.Client) *CrawlerBoardService {
	return &CrawlerBoardService{
		client,
	}
}

func (cb *CrawlerBoardService) GetSubmissions(ctx context.Context, order string, page, limit int) (*[]string, int64, error) {

	pipe := cb.redisClient.TxPipeline()

	var rangeCmd *redisLib.StringSliceCmd
	cardCmd := pipe.ZCard(ctx, redis.CrawlerboardKey)

	start := int64(limit * (page - 1))
	end := int64(limit*page - 1)

	if order == "old" {
		rangeCmd = pipe.ZRange(ctx, redis.CrawlerboardKey, start, end)
	} else {
		rangeCmd = pipe.ZRevRange(ctx, redis.CrawlerboardKey, start, end)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, 0, err
	}

	submissions, err := rangeCmd.Result()
	if err != nil {
		return nil, 0, err
	}

	total, err := cardCmd.Result()

	return &submissions, total, err
}

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, submissions *[]string) (*[]string, *map[string][]string, error) {
	sortedSetMembers := make(map[string]redisLib.Z, len(*submissions))
	failureSubmissionMap := make(map[string][]string, 2)

	pipe := cb.redisClient.TxPipeline()

	unsupportedSubmissions := make([]string, 0, len(*submissions))

	for _, submission := range *submissions {
		normalizedUrl, allowed, err := utils.NormalizeURL("", submission)
		if !allowed {
			if err != nil {
				log.Debug(err)
			}
			unsupportedSubmissions = append(unsupportedSubmissions, submission)

		} else {
			sortedSetMembers[submission] = redisLib.Z{
				Member: normalizedUrl,
				Score:  float64(time.Now().Unix()),
			}
		}
	}

	if len(unsupportedSubmissions) != 0 {
		failureSubmissionMap[MalformedUrls] = unsupportedSubmissions
	}

	if len(sortedSetMembers) == 0 {
		return &[]string{}, &failureSubmissionMap, nil
	}

	addCmds := make(map[*redisLib.IntCmd]string, len(sortedSetMembers))

	successful := make([]string, 0, len(sortedSetMembers))

	for submission, sortedSetMember := range sortedSetMembers {
		addCmds[pipe.ZAddNX(ctx, redis.CrawlerboardKey, sortedSetMember)] = submission
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, nil, err
	}

	duplicateSubmissions := make([]string, 0, len(sortedSetMembers))

	for addCmd, submission := range addCmds {
		val, err := addCmd.Result()
		if err != nil {
			return nil, nil, err
		}

		if val == 0 {
			duplicateSubmissions = append(duplicateSubmissions, submission)
		} else {
			successful = append(successful, submission)
		}
	}

	if len(duplicateSubmissions) != 0 {
		failureSubmissionMap[ConflictingUrls] = duplicateSubmissions
	}

	return &successful, &failureSubmissionMap, nil
}

func (cb *CrawlerBoardService) AcceptSubmissions(ctx context.Context, submissionsToAccept *[]string, order string, page, limit int) (*[]string, *map[string][]string, error) {
	successful := make([]string, 0, len(*submissionsToAccept))
	failureSubmissionMap := make(map[string][]string, 1)

	remCmds := make([]*redisLib.IntCmd, len(*submissionsToAccept))
	pipe := cb.redisClient.TxPipeline()

	for idx, submission := range *submissionsToAccept {
		remCmds[idx] = pipe.ZRem(ctx, redis.CrawlerboardKey, submission)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, nil, err
	}

	notFound := make([]string, 0, len(*submissionsToAccept))

	domainQueueMembers := make([]redisLib.Z, 0, len(*submissionsToAccept))
	domainsAndUrls := make(map[redisLib.Z][]any, len(*submissionsToAccept))

	for idx, cmd := range remCmds {
		val, err := cmd.Result()
		if err != nil {
			return nil, nil, err
		}

		if val == 0 {
			notFound = append(notFound, (*submissionsToAccept)[idx])
			continue
		}

		parsedUrl, _ := url.Parse((*submissionsToAccept)[idx])

		qMember := redisLib.Z{
			Member: parsedUrl.Host,
			Score:  float64(time.Now().Unix()),
		}
		domainQueueMembers = append(domainQueueMembers, qMember)
		domainsAndUrls[qMember] = append(domainsAndUrls[qMember], (*submissionsToAccept)[idx])
	}

	if len(notFound) != 0 {
		failureSubmissionMap[NotFoundUrls] = notFound
	}

	if len(domainQueueMembers) == 0 {
		return &successful, &failureSubmissionMap, nil
	}

	acceptPipe := cb.redisClient.TxPipeline()

	pushCmdsAndUrlsMap := make(map[*redisLib.IntCmd][]any, len(domainQueueMembers))

	domainsAddCmd := acceptPipe.ZAddNX(ctx, redis.DomainPendingQueue, domainQueueMembers...)

	for _, qMember := range domainQueueMembers {
		domainUrls := domainsAndUrls[qMember]
		pushCmdsAndUrlsMap[acceptPipe.LPush(ctx, "crawl_queue:"+qMember.Member.(string), domainUrls...)] = domainUrls
	}

	_, err = acceptPipe.Exec(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err = domainsAddCmd.Err(); err != nil {
		return nil, nil, err
	}

	for cmd, urls := range pushCmdsAndUrlsMap {
		_, err := cmd.Result()
		if err != nil {
			return nil, nil, err
		}

		successful = append(successful, utils.ToStringSlice(urls)...)
	}

	return &successful, &failureSubmissionMap, nil
}

func (cb *CrawlerBoardService) RejectSubmissions(ctx context.Context, submissionsToReject *[]string, order string, page, limit int) (*[]string, *map[string][]string, error) {
	successful := make([]string, 0, len(*submissionsToReject))
	failureSubmissionMap := make(map[string][]string, 1)

	remCmds := make([]*redisLib.IntCmd, len(*submissionsToReject))
	pipe := cb.redisClient.TxPipeline()

	for idx, submission := range *submissionsToReject {
		remCmds[idx] = pipe.ZRem(ctx, redis.CrawlerboardKey, submission)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, nil, err
	}

	notFound := make([]string, 0, len(*submissionsToReject))

	for idx, cmd := range remCmds {
		val, err := cmd.Result()
		if err != nil {
			return nil, nil, err
		}

		if val == 0 {
			notFound = append(notFound, (*submissionsToReject)[idx])
		} else {
			successful = append(successful, (*submissionsToReject)[idx])
		}
	}

	if len(notFound) != 0 {
		failureSubmissionMap[NotFoundUrls] = notFound
	}

	return &successful, &failureSubmissionMap, err
}
