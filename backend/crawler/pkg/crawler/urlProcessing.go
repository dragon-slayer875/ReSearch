package crawler

import (
	"bytes"
	"crawler/pkg/queue"
	"fmt"
	"io"
	"net/http"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/html"
)

func (crawler *Crawler) ProcessURL(urlString string) (*[]string, []byte, error) {
	resp, err := crawler.httpClient.Get(urlString)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("failed to fetch URL %s: %s", urlString, resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	candidateUrls := crawler.extractURLsFromHTML(urlString, bodyBytes)

	discoveredUrls, err := crawler.filterUnseenURLs(candidateUrls)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to filter unseen URLs: %w", err)
	}

	return discoveredUrls, bodyBytes, nil
}

func (crawler *Crawler) extractURLsFromHTML(baseURL string, htmlBytes []byte) []string {
	var candidateUrls []string
	tokenizer := html.NewTokenizer(bytes.NewReader(htmlBytes))

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			return candidateUrls // EOF
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						validatedUrl, err := validateUrl(baseURL, attr.Val)
						if err != nil {
							continue
						}

						isAllowed, err := isUrlOfAllowedResourceType(validatedUrl)
						if err != nil || !isAllowed {
							continue
						}

						candidateUrls = append(candidateUrls, validatedUrl)
						break
					}
				}
			}
		}
	}
}

func (crawler *Crawler) filterUnseenURLs(candidateUrls []string) (*[]string, error) {
	if len(candidateUrls) == 0 {
		return &[]string{}, nil
	}

	pipe := crawler.redisClient.Pipeline()

	saddCmds := make([]*redis.IntCmd, len(candidateUrls))
	for i, url := range candidateUrls {
		saddCmds[i] = pipe.SAdd(crawler.ctx, queue.SeenSet, url)
	}

	_, err := pipe.Exec(crawler.ctx)
	if err != nil {
		return nil, fmt.Errorf("redis pipeline failed: %w", err)
	}

	// Collect URLs that were newly added (return value 1 means new)
	discoveredUrls := make([]string, 0, len(candidateUrls))
	for i, cmd := range saddCmds {
		added, err := cmd.Result()
		if err != nil {
			crawler.logger.Printf("Failed to check URL %s: %v", candidateUrls[i], err)
			continue
		}
		if added == 1 { // New URL
			discoveredUrls = append(discoveredUrls, candidateUrls[i])
		}
	}

	return &discoveredUrls, nil
}
