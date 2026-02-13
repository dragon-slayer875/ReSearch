package utils

import (
	"fmt"
	"net/url"
	"path/filepath"
	english "server/internals/snowball"
	"slices"
	"strings"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
	snowball "github.com/snowballstem/snowball/go"
)

func StemWords(content []string) []string {
	var stemmedWords []string
	env := snowball.NewEnv("")
	english.Stem(env)

	for _, word := range content {
		env.SetCurrent(word)
		english.Stem(env)
		stemmedWords = append(stemmedWords, env.Current())
	}

	return stemmedWords
}

func RemoveStopWords(content []string) []string {
	var filteredWords []string

	for _, word := range content {
		word = strings.ToLower(word)
		if !isStopWord(word) {
			filteredWords = append(filteredWords, word)
		}
	}

	return filteredWords
}

func NormalizeURL(parentUrl, newUrl string) (string, bool, error) {
	pUrl, err := url.Parse(parentUrl)
	if parentUrl != "" && err != nil {
		return "", false, fmt.Errorf("could not parse parent URL: [%w]", err)
	}

	nUrl, err := url.Parse(newUrl)
	if err != nil {
		return "", false, fmt.Errorf("could not parse URL to normalize: [%w]", err)
	}

	// If the URL is relative, resolve it against the parent URL
	nUrl = pUrl.ResolveReference(nUrl)

	if nUrl.Scheme != "https" && nUrl.Scheme != "http" && nUrl.Scheme != "" {
		return "", false, fmt.Errorf("url has invalid 'Scheme'")
	}

	if nUrl.Host == "" {
		return "", false, fmt.Errorf("url has no 'Host'")
	}

	var normalizedURL string
	if nUrl.Scheme == "" {
		normalizedURL = "https" + "://" + nUrl.Host
	} else {
		normalizedURL = nUrl.Scheme + "://" + nUrl.Host
	}

	ext := ""
	allowed := true
	if nUrl.Path != "" {
		trimmedPath := strings.TrimSuffix(nUrl.Path, "/")
		ext = filepath.Ext(nUrl.Path)
		normalizedURL += trimmedPath
	}

	if ext != "" {
		allowed = isUrlOfAllowedResourceType(ext)
	}

	return normalizedURL, allowed, nil
}

func isUrlOfAllowedResourceType(ext string) bool {
	commonWebAndImgExtensions := []string{
		".html", ".htm", ".php", ".asp", ".aspx", ".jsp",
		".txt",
	}

	// 	".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
	// ".bmp", ".tiff", ".ico",

	return slices.Contains(commonWebAndImgExtensions, ext)
}

func Render(c fiber.Ctx, component templ.Component) error {
	c.Set("Content-Type", "text/html")
	return component.Render(c.Context(), c.Response().BodyWriter())
}

func ToAnySlice[Type any](slice []Type) []any {
	result := make([]any, len(slice))
	for idx, val := range slice {
		result[idx] = val
	}
	return result
}
