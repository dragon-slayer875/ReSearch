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

func (cb *CrawlerBoardService) GetSubmissions(ctx context.Context) ([]redisLib.Z, error) {
	return cb.redisClient.ZRevRangeWithScores(ctx, redis.CrawlerScoreBoard, 0, 9).Result()
}

func (cb *CrawlerBoardService) AddSubmissions(ctx context.Context, submissions []string) ([]string, error) {
	failedSubmissions := make([]string, 0, len(submissions))
	submissionsWithTimes := make([]redisLib.Z, 0, len(submissions))
	submissionsWithScores := make([]redisLib.Z, 0, len(submissions))

	pipe := cb.redisClient.TxPipeline()

	for _, submission := range submissions {
		normalizedUrl, allowed, _ := utils.NormalizeURL("", submission)
		if !allowed {
			failedSubmissions = append(failedSubmissions, submission)
			continue
		}

		submissionsWithScores = append(submissionsWithScores, redisLib.Z{
			Member: normalizedUrl,
			Score:  1,
		})
		submissionsWithTimes = append(submissionsWithTimes, redisLib.Z{
			Member: normalizedUrl,
			Score:  float64(time.Now().Unix()),
		})
	}

	if len(submissionsWithScores) == 0 {
		return failedSubmissions, nil
	}

	pipe.ZAddNX(ctx, redis.CrawlerScoreBoard, submissionsWithScores...)
	pipe.ZAddNX(ctx, redis.CrawlerTimeBoard, submissionsWithTimes...)

	_, err := pipe.Exec(ctx)

	return failedSubmissions, err
}

func (cb *CrawlerBoardService) UpdateVotes(ctx context.Context, submissions []string, voteIntent string) ([]string, error) {
	nonExistentSubmissions := make([]string, 0, len(submissions))
	existingSubmissions := make([]string, 0, len(submissions))

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

	if len(existingSubmissions) == 0 {
		return nonExistentSubmissions, nil
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
	nonExistentSubmissions := make([]string, 0, len(submissions))
	existingSubmissions := make([]any, 0, len(submissions))

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

	if len(existingSubmissions) == 0 {
		return nonExistentSubmissions, nil
	}

	acceptPipe := cb.redisClient.TxPipeline()
	acceptPipe.ZRem(ctx, redis.CrawlerScoreBoard, existingSubmissions...)

	domains := make([]redisLib.Z, 0)
	domainsAndUrls := make(map[redisLib.Z][]any)

	for _, submission := range existingSubmissions {
		// Only pre existing urls are operated on
		// Urls are normalized while initial queuing so they will always parse here
		parsedUrl, _ := url.Parse((submission).(string))

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
	nonExistentSubmissions := make([]string, 0, len(submissions))
	existingSubmissions := make([]any, 0, len(submissions))

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

	if len(existingSubmissions) == 0 {
		return nonExistentSubmissions, nil
	}

	rejectPipe := cb.redisClient.TxPipeline()
	rejectPipe.ZRem(ctx, redis.CrawlerScoreBoard, existingSubmissions...)

	_, err = rejectPipe.Exec(ctx)
	return nonExistentSubmissions, err
}
