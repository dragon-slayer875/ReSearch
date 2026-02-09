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
	errNotEnglishPage   = fmt.Errorf("not and english page")
	errNotValidResource = fmt.Errorf("not supported resource type")
	errNotOkayHttpCode  = fmt.Errorf("http status code not in range of 200 - 300")
)

func (worker *Worker) crawlUrl(page *utils.WebPage) (err error) {
	allowedResourceType := false

	var resp *http.Response
	err = worker.retryer.Do(worker.workerCtx, func() error {
		var tryErr error
		resp, tryErr = worker.httpClient.Get(page.Url)

		return tryErr
	}, utils.IsRetryableNetworkError)
	if err != nil {
		return err
	}

	defer func() {
		if deferErr := resp.Body.Close(); deferErr != nil {
			err = deferErr
		}
	}()

	if resp.StatusCode != http.StatusOK {
		page.HttpStatusCode = resp.StatusCode
		return errNotOkayHttpCode
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
		return errNotValidResource
	}

	htmlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	page.Outlinks, page.DomainQueueMembers, page.DomainAndUrls, err = worker.discoverAndQueueUrls(page, htmlBytes)
	if err != nil && err != io.EOF {
		return err
	}

	page.HtmlContent = &htmlBytes

	return nil
}

func (worker *Worker) discoverAndQueueUrls(page *utils.WebPage, htmlBytes []byte) (*[]string, *[]redisLib.Z, *map[string][]any, error) {
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
						normalizedURL, domain, allowed, err := utils.NormalizeURL(page.Url, attr.Val)
						if err != nil {
							worker.logger.Debug("Failed to normalize URL", zap.String("raw_url", attr.Val), zap.Error(err))
							continue
						}

						if !allowed {
							continue
						}

						if _, exists := uniqueUrls[normalizedURL]; exists || normalizedURL == page.Url {
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
