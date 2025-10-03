package crawler

import (
	"bytes"
	"crawler/pkg/queue"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/html"
)

var (
	errNotEnglishPage = fmt.Errorf("not an English page")
)

func (crawler *Crawler) ProcessURL(urlString string) (*[]string, []byte, error) {
	resp, err := crawler.httpClient.Get(urlString)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("HTTP error: %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	candidateUrls, err := crawler.extractURLsFromHTML(urlString, bodyBytes)
	if err != nil {
		return nil, nil, err
	}

	discoveredUrls, err := crawler.filterUnseenURLs(candidateUrls)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to filter unseen URLs: %w", err)
	}

	return discoveredUrls, bodyBytes, nil
}

func (crawler *Crawler) extractURLsFromHTML(baseURL string, htmlBytes []byte) (*[]string, error) {
	var candidateUrls []string
	tokenizer := html.NewTokenizer(bytes.NewReader(htmlBytes))

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			return &candidateUrls, nil // EOF
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()

			if token.Data == "html" {
				for _, attr := range token.Attr {
					if attr.Key == "lang" {
						lang := strings.ToLower(attr.Val)
						if lang != "en" && !strings.HasPrefix(lang, "en-") {
							return nil, errNotEnglishPage // Non-English page, skip processing
						}
					}
				}
			}

			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						validatedUrl, err := validateUrl(baseURL, attr.Val)
						if err != nil {
							crawler.logger.Println("Url validation error:", err)
							continue
						}

						isAllowed, err := isUrlOfAllowedResourceType(validatedUrl)
						if err != nil || !isAllowed {
							crawler.logger.Println("Url validation error:", err)
							continue
						}

						normalizedURL, err := normalizeURL(validatedUrl)
						if err != nil {
							crawler.logger.Println("Url normalization error:", err)
							continue
						}

						candidateUrls = append(candidateUrls, normalizedURL)
						break
					}
				}
			}
		}
	}
}

func (crawler *Crawler) filterUnseenURLs(candidateUrls *[]string) (*[]string, error) {
	if len(*candidateUrls) == 0 {
		return &[]string{}, nil
	}

	pipe := crawler.redisClient.Pipeline()

	saddCmds := make([]*redis.IntCmd, len(*candidateUrls))
	for i, url := range *candidateUrls {
		saddCmds[i] = pipe.SAdd(crawler.ctx, queue.SeenSet, url)
	}

	_, err := pipe.Exec(crawler.ctx)
	if err != nil {
		return nil, fmt.Errorf("redis pipeline failed: %w", err)
	}

	// Collect URLs that were newly added (return value 1 means new)
	discoveredUrls := make([]string, 0, len(*candidateUrls))
	for i, cmd := range saddCmds {
		added, err := cmd.Result()
		if err != nil {
			crawler.logger.Printf("Failed to check URL %s: %v", (*candidateUrls)[i], err)
			continue
		}
		if added == 1 { // New URL
			discoveredUrls = append(discoveredUrls, (*candidateUrls)[i])
		}
	}

	return &discoveredUrls, nil
}

func normalizeURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)

	if err != nil {
		return "", fmt.Errorf("could not parse raw URL [%w]", err)
	}

	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("url has invalid field 'Scheme'")
	}

	if u.Host == "" {
		return "", fmt.Errorf("url has no field 'Host'")
	}

	normalizedURL := u.Scheme + "://" + u.Host

	if u.Path != "" {
		trimmedPath := strings.TrimSuffix(u.Path, "/")
		normalizedURL += trimmedPath
	}

	return normalizedURL, nil
}
