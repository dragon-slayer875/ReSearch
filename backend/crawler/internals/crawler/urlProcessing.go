package crawler

import (
	"bytes"
	"crawler/internals/storage/redis"
	"crawler/internals/utils"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	redisLib "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/net/html"
)

var (
	errNotEnglishPage   = fmt.Errorf("not an English page")
	errNotValidResource = fmt.Errorf("not a valid web page")
)

func (worker *Worker) crawlUrl(url, domain string) (links *[]string, htmlBytes []byte, err error) {
	allowedResourceType := false

	var resp *http.Response
	err = worker.retryer.Do(worker.workerCtx, func() error {
		var tryErr error
		resp, tryErr = worker.httpClient.Get(url)

		return tryErr
	}, utils.IsRetryableNetworkError)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if deferErr := resp.Body.Close(); deferErr != nil {
			err = deferErr
		}
	}()

	if err = worker.redisClient.HSet(worker.workerCtx, "crawl:domain_delays", domain, time.Now().Unix()).Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to update domain delay: %w", err)
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

	htmlBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	links, err = worker.discoverAndQueueUrls(url, htmlBytes)
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

			err := worker.retryer.Do(worker.workerCtx, func() error {
				_, err := pipe.Exec(worker.workerCtx)
				return err
			}, utils.IsRetryableRedisError)

			if err != nil {
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
						normalizedURL, domain, ext, err := utils.NormalizeURL(baseURL, attr.Val)
						if err != nil {
							worker.logger.Warn("Failed to normalize URL", zap.String("raw_url", attr.Val), zap.String("warning", err.Error()))
							continue
						}

						if ext != "" {
							isAllowed := utils.IsUrlOfAllowedResourceType(normalizedURL)
							if !isAllowed {
								continue
							}
						}

						if _, exists := uniqueUrls[normalizedURL]; exists || normalizedURL == baseURL {
							continue
						}

						uniqueUrls[normalizedURL] = struct{}{}
						outlinks = append(outlinks, normalizedURL)

						pipe.ZAddNX(worker.workerCtx, redis.DomainPendingQueue, redisLib.Z{
							Member: domain,
							Score:  float64(time.Now().Unix()),
						})
						pipe.LPush(worker.workerCtx, "crawl_queue:"+domain, normalizedURL)
						worker.logger.Debug("Piped URL", zap.String("normalized_url", normalizedURL))

						break
					}
				}
			}
		}
	}
}
