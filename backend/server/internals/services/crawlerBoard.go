package services

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"server/internals/storage/redis"
	"server/internals/utils"
	"strconv"
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

func (cb *CrawlerBoardService) GetSubmissions(ctx context.Context, sort, order string, page, limit int) (redisLib.FTSearchResult, error) {
	asc := false
	dsc := true

	if order == "asc" {
		asc = true
		dsc = false
	}

	return cb.redisClient.FTSearchWithArgs(ctx, redis.CrawlerBoardIdx, "*", &redisLib.FTSearchOptions{
		Limit:       limit,
		LimitOffset: limit * (page - 1),
		SortBy: []redisLib.FTSearchSortBy{
			{
				FieldName: sort,
				Asc:       asc,
				Desc:      dsc,
			},
		},
		Return: []redisLib.FTSearchReturn{
			{
				FieldName: "__key",
				As:        "id",
			}, {
				FieldName: "url",
			}, {
				FieldName: "score",
			}, {
				FieldName: "time",
			}}}).Result()
}

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, submissions []string) ([]string, error) {
	successfulSubmissions := make([]string, 0, len(submissions))

	pipe := cb.redisClient.TxPipeline()

	for _, submission := range submissions {
		normalizedUrl, allowed, _ := utils.NormalizeURL("", submission)
		if !allowed {
			continue
		}

		urlHash := sha256.Sum256([]byte(normalizedUrl))
		urlHashStr := fmt.Sprintf("%x", urlHash)

		pipe.HSetEXWithArgs(ctx, redis.CrawlerBoardPrefix+urlHashStr, &redisLib.HSetEXOptions{
			Condition: redisLib.HSetEXCondition(redisLib.HSetEXFNX),
		}, "url", normalizedUrl,
			"score", strconv.Itoa(1),
			"time", strconv.FormatInt(time.Now().Unix(), 10))
		successfulSubmissions = append(successfulSubmissions, submission)
	}

	_, err := pipe.Exec(ctx)

	return successfulSubmissions, err
}

func (cb *CrawlerBoardService) UpdateVotes(ctx context.Context, submissions []string, voteIntent string) (int, error) {
	// TODO: think of further ways to remove the need of looping twice
	var successfulSubs int
	existenceCmds := make(map[string]*redisLib.StringCmd, len(submissions))

	existencePipe := cb.redisClient.TxPipeline()
	for _, submission := range submissions {
		existenceCmds[submission] = existencePipe.HGet(ctx, submission, "url")
	}

	_, err := existencePipe.Exec(ctx)
	if err != nil && err != redisLib.Nil {
		return successfulSubs, err
	}

	vote := 1
	if voteIntent == "down" {
		vote = -1
	}

	pipe := cb.redisClient.TxPipeline()

	for submission, existenceCmd := range existenceCmds {
		val, err := existenceCmd.Result()
		if err != nil && err != redisLib.Nil {
			return successfulSubs, err
		}

		if val == "" {
			continue
		}

		pipe.HIncrBy(ctx, submission, "score", int64(vote))
		successfulSubs++
	}

	_, err = pipe.Exec(ctx)
	return successfulSubs, err
}

func (cb *CrawlerBoardService) AcceptSubmissions(ctx context.Context, submissions []string) (int, error) {
	var successfulSubs int
	redisCmds := make([]*redisLib.StringSliceCmd, 0, len(submissions))

	pipe := cb.redisClient.TxPipeline()

	for _, submission := range submissions {
		redisCmds = append(redisCmds, pipe.HGetDel(ctx, submission, "url", "score", "time"))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return successfulSubs, err
	}

	domains := make([]redisLib.Z, 0)
	domainsAndUrls := make(map[redisLib.Z][]any)

	for _, cmd := range redisCmds {
		vals, err := cmd.Result()
		if err != nil {
			return successfulSubs, err
		}

		if vals[0] == "nil" {
			continue
		}

		parsedUrl, _ := url.Parse(vals[0])

		domain := redisLib.Z{
			Member: parsedUrl.Hostname(),
			Score:  float64(time.Now().Unix()),
		}
		domains = append(domains, domain)
		domainsAndUrls[domain] = append(domainsAndUrls[domain], vals[0])
	}

	acceptPipe := cb.redisClient.TxPipeline()

	acceptPipe.ZAddNX(ctx, redis.DomainPendingQueue, domains...)

	for _, domain := range domains {
		acceptPipe.LPush(ctx, "crawl_queue:"+domain.Member.(string), domainsAndUrls[domain]...)
	}

	_, err = acceptPipe.Exec(ctx)
	return successfulSubs, err
}

func (cb *CrawlerBoardService) RejectSubmissions(ctx context.Context, submissions []string) (int64, error) {
	pipe := cb.redisClient.TxPipeline()
	scoresCmd := pipe.Del(ctx, submissions...)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	result, err := scoresCmd.Result()

	return result, err
}
