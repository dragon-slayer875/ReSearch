package crawler

import (
	"bufio"
	"context"
	"crawler/internals/storage/redis"
	"crawler/internals/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

type RobotRules struct {
	Allow      []string `json:"allow"`
	Disallow   []string `json:"disallow"`
	CrawlDelay int      `json:"crawl_delay"`
	Sitemaps   []string `json:"sitemaps"`
}

func defaultRobotRules() *RobotRules {
	return &RobotRules{
		Allow:      []string{},
		Disallow:   []string{},
		CrawlDelay: 10,
		Sitemaps:   []string{},
	}
}

func (worker *Worker) fetchRobotRulesFromWeb(domain string) (robotRules *RobotRules, err error) {
	domainPrefix := "https://" + domain
	var resp *http.Response
	err = worker.retryer.Do(worker.workerCtx, func() error {
		var tryErr error
		resp, tryErr = worker.httpClient.Get(domainPrefix + "/robots.txt")
		if tryErr == http.ErrSchemeMismatch {
			domainPrefix = "http://" + domain
		}

		return tryErr
	}, utils.IsRetryableNetworkError)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTeapot {
		worker.logger.Warn("Non OK status code received for robots.txt", zap.Int("status_code", resp.StatusCode))
		return nil, nil
	}

	defer func() {
		if deferErr := resp.Body.Close(); deferErr != nil {
			err = deferErr
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
	foundUserAgentAll := false
	robotRules = defaultRobotRules()

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lowercaseLine := strings.ToLower(line)

		if lowercaseLine == "" || strings.HasPrefix(lowercaseLine, "#") {
			continue
		}

		if lowercaseLine == "user-agent: *" {
			foundUserAgentAll = true
			continue
		}

		if foundUserAgentAll {
			if strings.HasPrefix(lowercaseLine, "user-agent:") {
				break
			}

			if allowPath, found := strings.CutPrefix(lowercaseLine, "allow:"); found {
				if strings.TrimSpace(allowPath) == "" {
					continue
				}
				robotRules.Allow = append(robotRules.Allow, domainPrefix+allowPath)
			}

			if disallowPath, found := strings.CutPrefix(lowercaseLine, "disallow:"); found {
				if strings.TrimSpace(disallowPath) == "" {
					continue
				}
				robotRules.Allow = append(robotRules.Allow, domainPrefix+disallowPath)
			}

			if crawlDelayStr, found := strings.CutPrefix(lowercaseLine, "crawl-delay:"); found {
				if strings.TrimSpace(crawlDelayStr) == "" {
					continue
				}
				crawlDelay, err := strconv.Atoi(crawlDelayStr)
				if err != nil {
					worker.logger.Warn("Failed to parse crawl delay in robots.txt", zap.Error(err))
					continue
				}
				robotRules.CrawlDelay = crawlDelay
			}

			if sitemapUrl, found := strings.CutPrefix(lowercaseLine, "sitemap:"); found {
				if strings.TrimSpace(sitemapUrl) == "" {
					continue
				}
				robotRules.Sitemaps = append(robotRules.Sitemaps, sitemapUrl)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error parsing robots.txt: %w", err)
	}

	return robotRules, nil
}

func (robotRules *RobotRules) isPolite(ctx context.Context, domainString string, redisClient *redis.RedisClient) (bool, error) {
	lastCrawlTime, err := redisClient.HGet(ctx, "crawl:domain_delays", domainString).Int64()
	if err != nil {
		return true, err
	}

	minDelay := time.Duration(robotRules.CrawlDelay) * time.Second
	timeSinceLastCrawl := time.Since(time.Unix(lastCrawlTime, 0))

	return timeSinceLastCrawl >= minDelay, nil
}

func (robotRules *RobotRules) isAllowed(url string) bool {
	for _, allowedPath := range robotRules.Allow {
		if strings.HasPrefix(url, allowedPath) {
			return true
		}
	}

	for _, disallowedPath := range robotRules.Disallow {
		if strings.HasPrefix(url, disallowedPath) {
			return false
		}
	}

	return true
}

func (worker *Worker) parseRobotRules(robotRulesJson []byte) *RobotRules {
	var robotRules RobotRules

	if len(robotRulesJson) == 0 {
		return nil
	}

	if err := json.Unmarshal(robotRulesJson, &robotRules); err != nil {
		worker.logger.Warn("Failed to parse robot rules", zap.Error(err))
		return nil
	}

	return &robotRules
}
