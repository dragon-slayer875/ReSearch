package services

import (
	"context"
	"net/url"
	"server/internals/storage/redis"
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

func (cb *CrawlerBoardService) GetSubmissions(ctx context.Context) ([]redisLib.Z, error) {
	return cb.redisClient.ZRevRangeWithScores(ctx, redis.CrawlerScoreBoard, 0, 9).Result()
}

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, entries []string) error {
	pipe := cb.redisClient.TxPipeline()
	var submissionsWithTimes []redisLib.Z
	var submissionsWithScores []redisLib.Z

	for _, entry := range entries {
		submissionsWithScores = append(submissionsWithScores, redisLib.Z{
			Member: entry,
			Score:  1,
		})
		submissionsWithTimes = append(submissionsWithTimes, redisLib.Z{
			Member: entry,
			Score:  float64(time.Now().Unix()),
		})
	}

	pipe.ZAddNX(ctx, redis.CrawlerScoreBoard, submissionsWithScores...)
	pipe.ZAddNX(ctx, redis.CrawlerTimeBoard, submissionsWithTimes...)

	_, err := pipe.Exec(ctx)

	return err
}

func (cb *CrawlerBoardService) UpdateVotes(ctx context.Context, submissions []string, voteIntent string) ([]string, error) {
	nonExistentSubmissions := make([]string, 0)
	existingSubmissions := make([]string, 0)

	pipe := cb.redisClient.TxPipeline()
	scoresCmd := pipe.ZMScore(ctx, redis.CrawlerScoreBoard, submissions...)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nonExistentSubmissions, err
	}

	scores, err := scoresCmd.Result()
	if err != nil {
		return nonExistentSubmissions, err
	}

	for idx, score := range scores {
		if score == 0 {
			nonExistentSubmissions = append(nonExistentSubmissions, submissions[idx])
		} else {
			existingSubmissions = append(existingSubmissions, submissions[idx])
		}
	}

	updatesPipe := cb.redisClient.TxPipeline()

	vote := 1
	if voteIntent == "down" {
		vote = -1
	}

	for _, submission := range existingSubmissions {
		updatesPipe.ZIncrBy(ctx, redis.CrawlerScoreBoard, float64(vote), submission)
	}

	_, err = updatesPipe.Exec(ctx)
	return nonExistentSubmissions, err
}

func (cb *CrawlerBoardService) AcceptSubmissions(ctx context.Context, submissions []string) ([]string, error) {
	nonExistentSubmissions := make([]string, 0)
	existingSubmissions := make([]any, 0)

	pipe := cb.redisClient.TxPipeline()
	scoresCmd := pipe.ZMScore(ctx, redis.CrawlerScoreBoard, submissions...)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nonExistentSubmissions, err
	}

	scores, err := scoresCmd.Result()
	if err != nil {
		return nonExistentSubmissions, err
	}

	for idx, score := range scores {
		if score == 0 {
			nonExistentSubmissions = append(nonExistentSubmissions, submissions[idx])
		} else {
			existingSubmissions = append(existingSubmissions, submissions[idx])
		}
	}

	acceptPipe := cb.redisClient.TxPipeline()
	acceptPipe.ZRem(ctx, redis.CrawlerScoreBoard, existingSubmissions...)

	domains := make([]redisLib.Z, 0)
	domainsAndUrls := make(map[redisLib.Z][]any)

	for _, submission := range existingSubmissions {
		parsedUrl, err := url.Parse((submission).(string))
		if err != nil {
			return nonExistentSubmissions, err
		}

		domain := redisLib.Z{
			Member: parsedUrl.Hostname(),
			Score:  float64(time.Now().Unix()),
		}
		domains = append(domains, domain)
		domainsAndUrls[domain] = append(domainsAndUrls[domain], submission)
	}

	acceptPipe.ZAddNX(ctx, redis.DomainPendingQueue, domains...)

	for _, domain := range domains {
		acceptPipe.LPush(ctx, "crawl_queue:"+domain.Member.(string), domainsAndUrls[domain]...)
	}

	_, err = acceptPipe.Exec(ctx)
	return nonExistentSubmissions, err
}

func (cb *CrawlerBoardService) RejectSubmissions(ctx context.Context, submissions []string) ([]string, error) {
	nonExistentSubmissions := make([]string, 0)
	existingSubmissions := make([]any, 0)

	pipe := cb.redisClient.TxPipeline()
	scoresCmd := pipe.ZMScore(ctx, redis.CrawlerScoreBoard, submissions...)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nonExistentSubmissions, err
	}

	scores, err := scoresCmd.Result()
	if err != nil {
		return nonExistentSubmissions, err
	}

	for idx, score := range scores {
		if score == 0 {
			nonExistentSubmissions = append(nonExistentSubmissions, submissions[idx])
		} else {
			existingSubmissions = append(existingSubmissions, submissions[idx])
		}
	}

	rejectPipe := cb.redisClient.TxPipeline()
	rejectPipe.ZRem(ctx, redis.CrawlerScoreBoard, existingSubmissions...)

	_, err = rejectPipe.Exec(ctx)
	return nonExistentSubmissions, err
}
