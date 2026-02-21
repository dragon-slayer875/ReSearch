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
	urlsUnsupported = "Unsupported or malformed urls"
	urlsExist       = "Urls already exist"
	urlsAddErr      = "Error occured while adding url"
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

	for _, submission := range *submissions {
		normalizedUrl, allowed, err := utils.NormalizeURL("", submission)
		if !allowed {
			if err != nil {
				log.Debug(err)
			}
			failureSubmissionMap[urlsUnsupported] = append(failureSubmissionMap[urlsUnsupported], submission)

		} else {
			sortedSetMembers[submission] = redisLib.Z{
				Member: normalizedUrl,
				Score:  float64(time.Now().Unix()),
			}
		}
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

	for addCmd, submission := range addCmds {
		val, err := addCmd.Result()
		if err != nil {
			failureSubmissionMap[urlsAddErr] = append(failureSubmissionMap[urlsAddErr], submission)
			continue
		}

		if val == 0 {
			failureSubmissionMap[urlsExist] = append(failureSubmissionMap[urlsExist], submission)
		} else {
			successfulSubmissions = append(successfulSubmissions, submission)
		}
	}

	return &successfulSubmissions, &failureSubmissionMap, nil
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
	transformedSubmissions := utils.ToAnySlice(submissions)
	return cb.redisClient.ZRem(ctx, redis.CrawlerboardKey, transformedSubmissions...).Result()
}

func (cb *CrawlerBoardService) RejectAndGetSubmissions(ctx context.Context, submissionsToReject []string, order string, page, limit int) (*[]string, error) {
	pipe := cb.redisClient.TxPipeline()
	transformedSubmissions := utils.ToAnySlice(submissionsToReject)
	pipe.ZRem(ctx, redis.CrawlerboardKey, transformedSubmissions...)

	getCmd := new(redisLib.StringSliceCmd)

	if order == "asc" {
		getCmd = pipe.ZRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page))
	} else {
		getCmd = pipe.ZRevRange(ctx, redis.CrawlerboardKey, int64(limit*(page-1)), int64(limit*page))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	submissions, err := getCmd.Result()
	return &submissions, err
}
