package crawler

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RobotRules struct {
	Allow      []string `json:"allow"`
	Disallow   []string `json:"disallow"`
	CrawlDelay int      `json:"crawl_delay"`
	Sitemaps   []string `json:"sitemaps"`
}

type RobotRulesError struct {
	CacheError error
	DBError    error
	WebError   error
	LockError  error
}

var rulesLockedError = fmt.Errorf("rules lock already held by another worker")

func (e *RobotRulesError) Error() string {
	var parts []string
	if e.CacheError != nil {
		parts = append(parts, fmt.Sprintf("cache: %v", e.CacheError))
	}
	if e.DBError != nil {
		parts = append(parts, fmt.Sprintf("db: %v", e.DBError))
	}
	if e.WebError != nil {
		parts = append(parts, fmt.Sprintf("web: %v", e.WebError))
	}
	if e.LockError != nil {
		parts = append(parts, fmt.Sprintf("lock: %v", e.LockError))
	}
	return "robot rules errors: " + strings.Join(parts, ", ")
}

func (e *RobotRulesError) HasErrors() bool {
	return e.CacheError != nil || e.DBError != nil || e.WebError != nil || e.LockError != nil
}

func conditionalError(accErr *RobotRulesError) error {
	if accErr.HasErrors() {
		return accErr
	}
	return nil
}

func defaultRobotRules() *RobotRules {
	return &RobotRules{
		Allow:      []string{},
		Disallow:   []string{},
		CrawlDelay: 10,
		Sitemaps:   []string{},
	}
}

func fetchRobotRulesFromWeb(domainString string, httpClient *http.Client) (*RobotRules, error) {
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/robots.txt", domainString))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch robots.txt for %s: %w", domainString, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTeapot {
		return nil, fmt.Errorf("robots.txt not found for %s: %s", domainString, resp.Status)
	}

	robotTxtBody := resp.Body
	defer robotTxtBody.Close()

	scanner := bufio.NewScanner(robotTxtBody)
	foundUserAgentAll := false
	robotRules := defaultRobotRules()

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.ToLower(line) == "user-agent: *" {
			foundUserAgentAll = true
			continue
		}

		if foundUserAgentAll {
			if strings.HasPrefix(strings.ToLower(line), "user-agent:") {
				break
			}

			if strings.HasPrefix(strings.ToLower(line), "allow:") {
				allowPath := strings.TrimSpace(strings.TrimPrefix(line, "Allow:"))
				allowPath = fmt.Sprintf("http://%s%s", domainString, allowPath)
				if allowPath != "" {
					robotRules.Allow = append(robotRules.Allow, allowPath)
				}
			}

			if strings.HasPrefix(strings.ToLower(line), "disallow:") {
				disallowPath := strings.TrimSpace(strings.TrimPrefix(line, "Disallow:"))
				disallowPath = fmt.Sprintf("http://%s%s", domainString, disallowPath)
				if disallowPath != "" {
					robotRules.Disallow = append(robotRules.Disallow, disallowPath)
				}
			}

			if strings.HasPrefix(strings.ToLower(line), "crawl-delay:") {
				crawlDelayStr := strings.TrimSpace(strings.TrimPrefix(line, "Crawl-delay:"))
				if crawlDelayStr != "" {
					crawlDelay, err := strconv.Atoi(crawlDelayStr)
					if err == nil {
						robotRules.CrawlDelay = crawlDelay
					}
				}
			}

			if strings.HasPrefix(strings.ToLower(line), "sitemap:") {
				sitemapUrl := strings.TrimSpace(strings.TrimPrefix(line, "Sitemap:"))
				if sitemapUrl != "" {
					robotRules.Sitemaps = append(robotRules.Sitemaps, sitemapUrl)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error parsing robots.txt: %w", err)
	}

	return robotRules, nil
}

func (robotRules *RobotRules) isPolite(domainString string, redisClient *redis.Client) bool {
	lastCrawlTime, err := redisClient.HGet(context.Background(), "crawl:domain_delays", domainString).Int64()
	if err != nil {
		return true
	}

	minDelay := time.Duration(robotRules.CrawlDelay) * time.Second
	timeSinceLastCrawl := time.Since(time.Unix(lastCrawlTime, 0))

	return timeSinceLastCrawl >= minDelay
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
