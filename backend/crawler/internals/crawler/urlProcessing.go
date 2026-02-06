package crawler

import (
	"bytes"
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

func (worker *Worker) crawlUrl(url string) (outlinks *[]string, htmlBytes []byte, domainQueueMembers *[]redisLib.Z, domainsAndUrls *map[string][]any, err error) {
	allowedResourceType := false

	var resp *http.Response
	err = worker.retryer.Do(worker.workerCtx, func() error {
		var tryErr error
		resp, tryErr = worker.httpClient.Get(url)

		return tryErr
	}, utils.IsRetryableNetworkError)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	defer func() {
		if deferErr := resp.Body.Close(); deferErr != nil {
			err = deferErr
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, nil, nil, fmt.Errorf("HTTP error: %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
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
		return nil, nil, nil, nil, errNotValidResource
	}

	htmlBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	outlinks, domainQueueMembers, domainsAndUrls, err = worker.discoverAndQueueUrls(url, htmlBytes)
	if err != nil && err != io.EOF {
		return nil, nil, nil, nil, err
	}

	return outlinks, htmlBytes, domainQueueMembers, domainsAndUrls, nil
}

func (worker *Worker) discoverAndQueueUrls(baseURL string, htmlBytes []byte) (*[]string, *[]redisLib.Z, *map[string][]any, error) {
	uniqueUrls := make(map[string]struct{})
	domainsAndUrls := make(map[string][]any)
	domainQueueMembers := make([]redisLib.Z, 0)
	outlinks := make([]string, 0)
	tokenizer := html.NewTokenizer(bytes.NewReader(htmlBytes))

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			if err := tokenizer.Err(); err != io.EOF {
				return nil, nil, nil, err
			}

			return &outlinks, &domainQueueMembers, &domainsAndUrls, tokenizer.Err()
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()

			if token.Data == "html" {
				for _, attr := range token.Attr {
					if attr.Key == "lang" {
						lang := strings.ToLower(attr.Val)
						if lang != "en" && !strings.HasPrefix(lang, "en-") {
							return nil, nil, nil, errNotEnglishPage // Non-English page, skip processing
						}
					}
				}
			}

			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						normalizedURL, domain, allowed, err := utils.NormalizeURL(baseURL, attr.Val)
						if err != nil {
							worker.logger.Warn("Failed to normalize URL", zap.String("raw_url", attr.Val), zap.Error(err))
							continue
						}

						if !allowed {
							continue
						}

						if _, exists := uniqueUrls[normalizedURL]; exists || normalizedURL == baseURL {
							continue
						}

						uniqueUrls[normalizedURL] = struct{}{}
						outlinks = append(outlinks, normalizedURL)

						domainQueueMembers = append(domainQueueMembers, redisLib.Z{
							Member: domain,
							Score:  float64(time.Now().Unix()),
						})
						domainsAndUrls[domain] = append(domainsAndUrls[domain], normalizedURL)

						worker.logger.Debug("Found url", zap.String("normalized_url", normalizedURL))
						break
					}
				}
			}
		}
	}
}
