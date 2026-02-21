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

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, submissions *[]string) (*[]string, *map[string][]string, error) {
	sortedSetMembers := make(map[string]redisLib.Z, len(*submissions))
	failureSubmissionMap := make(map[string][]string, 3)

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
		failureSubmissionMap["Unsupported/malformed urls"] = unsupportedSubmissions
	}

	if len(sortedSetMembers) == 0 {
		return &[]string{}, &failureSubmissionMap, nil
	}

	addCmds := make(map[*redisLib.IntCmd]string, len(sortedSetMembers))

	successfulSubmissions := make([]string, 0, len(sortedSetMembers))

	for submission, sortedSetMember := range sortedSetMembers {
		addCmds[pipe.ZAddNX(ctx, redis.CrawlerboardKey, sortedSetMember)] = submission
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, nil, err
	}

	erroredSubmissions := make([]string, 0, len(sortedSetMembers))
	duplicateSubmissions := make([]string, 0, len(sortedSetMembers))

	for addCmd, submission := range addCmds {
		val, err := addCmd.Result()
		if err != nil {
			erroredSubmissions = append(erroredSubmissions, submission)
			continue
		}

		if val == 0 {
			duplicateSubmissions = append(duplicateSubmissions, submission)
		} else {
			successfulSubmissions = append(successfulSubmissions, submission)
		}
	}

	if len(duplicateSubmissions) != 0 {
		failureSubmissionMap["Urls already exist"] = duplicateSubmissions
	}

	if len(erroredSubmissions) != 0 {
		failureSubmissionMap["Error occured while adding urls"] = erroredSubmissions
	}

	return &successfulSubmissions, &failureSubmissionMap, nil
}

func (cb *CrawlerBoardService) AcceptSubmissions(ctx context.Context, submissionsToAccept *[]string, order string, page, limit int) (*[]string, int, *map[string][]string, error) {
	var successful int
	failureSubmissionMap := make(map[string][]string, 1)

	remCmds := make([]*redisLib.IntCmd, len(*submissionsToAccept))
	pipe := cb.redisClient.TxPipeline()

	for idx, submission := range *submissionsToAccept {
		remCmds[idx] = pipe.ZRem(ctx, redis.CrawlerboardKey, submission)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, successful, nil, err
	}

	notFound := make([]string, 0, len(*submissionsToAccept))

	domainQueueMembers := make([]redisLib.Z, 0, len(*submissionsToAccept))
	domainsAndUrls := make(map[redisLib.Z][]any, len(*submissionsToAccept))

	for idx, cmd := range remCmds {
		val, err := cmd.Result()
		if err != nil {
			return nil, successful, nil, err
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
		failureSubmissionMap["Urls not found"] = notFound
	}

	getCmd := pipe.ZRevRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page))
	if order == "asc" {
		getCmd = pipe.ZRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page))
	}

	submissions, err := getCmd.Result()
	if err != nil {
		return nil, successful, nil, err
	}

	if len(domainQueueMembers) == 0 {
		return &submissions, successful, &failureSubmissionMap, nil
	}

	acceptPipe := cb.redisClient.TxPipeline()

	pushCmdsAndUrlsMap := make(map[*redisLib.IntCmd][]any, len(domainQueueMembers))

	domainsAddCmd := acceptPipe.ZAddNX(ctx, redis.DomainPendingQueue, domainQueueMembers...)

	for _, qMember := range domainQueueMembers {
		pushCmdsAndUrlsMap[acceptPipe.LPush(ctx, "crawl_queue:"+qMember.Member.(string), domainsAndUrls[qMember]...)] = domainsAndUrls[qMember]
	}

	_, err = acceptPipe.Exec(ctx)
	if err != nil {
		return nil, successful, nil, err
	}

	if err = domainsAddCmd.Err(); err != nil {
		return nil, successful, nil, err
	}

	for cmd, urls := range pushCmdsAndUrlsMap {
		_, err := cmd.Result()
		if err != nil {
			return nil, successful, nil, err
		}

		successful += len(urls)
	}

	return &submissions, successful, &failureSubmissionMap, nil
}

func (cb *CrawlerBoardService) RejectAndGetSubmissions(ctx context.Context, submissionsToReject *[]string, order string, page, limit int) (*[]string, *[]string, *map[string][]string, error) {
	successfulRejections := make([]string, 0, len(*submissionsToReject))
	failureSubmissionMap := make(map[string][]string, 2)

	remCmds := make([]*redisLib.IntCmd, len(*submissionsToReject))
	pipe := cb.redisClient.TxPipeline()

	for idx, submission := range *submissionsToReject {
		remCmds[idx] = pipe.ZRem(ctx, redis.CrawlerboardKey, submission)
	}

	getCmd := pipe.ZRevRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page))
	if order == "asc" {
		getCmd = pipe.ZRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	errors := make([]string, 0, len(*submissionsToReject))
	notFound := make([]string, 0, len(*submissionsToReject))

	for idx, cmd := range remCmds {
		val, err := cmd.Result()
		if err != nil {
			errors = append(errors, (*submissionsToReject)[idx])
			continue
		}

		if val == 0 {
			notFound = append(notFound, (*submissionsToReject)[idx])
		} else {
			successfulRejections = append(successfulRejections, (*submissionsToReject)[idx])
		}
	}

	if len(notFound) != 0 {
		failureSubmissionMap["Urls not found"] = notFound
	}

	if len(errors) != 0 {
		failureSubmissionMap["Error occured while rejecting urls"] = errors
	}

	submissions, err := getCmd.Result()
	return &submissions, &successfulRejections, &failureSubmissionMap, err
}
