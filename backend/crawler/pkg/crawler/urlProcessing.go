package crawler

import (
	"bytes"
	"crawler/pkg/queue"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

var (
	errNotEnglishPage   = fmt.Errorf("not an English page")
	errNotValidResource = fmt.Errorf("not a valid web page")
)

func (worker *Worker) ProcessURL(url, domain string) (*[]string, []byte, error) {
	allowedResourceType := false

	resp, err := worker.httpClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if err := worker.redisClient.HSet(worker.ctx, "crawl:domain_delays", domain, time.Now().Unix()).Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to update domain delay for %s: %w", domain, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("HTTP error: %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	contentType := resp.Header.Get("Content-Type")
	validTypes := []string{
		"text/html",
		"text/plain",
		"application/xhtml+xml",
		//"image/jpeg",
		//"image/png",
		//"image/gif",
		//"image/webp",
		//"image/svg+xml",
	}

	for _, validType := range validTypes {
		if strings.HasPrefix(contentType, validType) {
			allowedResourceType = true
		}
	}

	if !allowedResourceType {
		return nil, nil, errNotValidResource
	}

	htmlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	links, err := worker.discoverAndQueueUrls(url, htmlBytes)
	if err != nil && err != io.EOF {
		return nil, nil, err
	}

	return links, htmlBytes, nil
}

func (worker *Worker) discoverAndQueueUrls(baseURL string, htmlBytes []byte) (*[]string, error) {
	uniqueUrls := make(map[string]struct{})
	outlinks := make([]string, 0)
	pipe := worker.redisClient.Pipeline()
	tokenizer := html.NewTokenizer(bytes.NewReader(htmlBytes))

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			if err := tokenizer.Err(); err != io.EOF {
				return nil, err
			}

			if _, err := pipe.Exec(worker.ctx); err != nil {
				return nil, err
			}

			return &outlinks, tokenizer.Err()
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
							worker.logger.Errorln("Url validation error:", err)
							continue
						}

						isAllowed, err := isUrlOfAllowedResourceType(validatedUrl)
						if err != nil || !isAllowed {
							if err != nil {
								worker.logger.Errorln("Url validation error:", err)
							}
							continue
						}

						normalizedURL, err := normalizeURL(validatedUrl)
						if err != nil {
							worker.logger.Errorln("Url normalization error:", err)
							continue
						}

						if _, exists := uniqueUrls[normalizedURL]; exists || normalizedURL == baseURL {
							continue
						}

						uniqueUrls[normalizedURL] = struct{}{}
						outlinks = append(outlinks, normalizedURL)
						pipe.ZIncrBy(worker.ctx, queue.PendingQueue, 1, normalizedURL)

						break
					}
				}
			}
		}
	}
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
