package crawler

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

func normalizeURL(parentUrl, newUrl string) (string, string, error) {
	pUrl, err := url.Parse(parentUrl)
	if err != nil && parentUrl != "" {
		return "", "", fmt.Errorf("could not parse parent URL [%w]", err)
	}

	nUrl, err := url.Parse(newUrl)
	if err != nil {
		return "", "", fmt.Errorf("could not parse new URL [%w]", err)
	}

	// If the URL is relative, resolve it against the parent URL
	nUrl = pUrl.ResolveReference(nUrl)

	if nUrl.Scheme != "https" && nUrl.Scheme != "http" {
		return "", "", fmt.Errorf("url has invalid 'Scheme'")
	}

	if nUrl.Host == "" {
		return "", "", fmt.Errorf("url has no 'Host'")
	}

	normalizedURL := nUrl.Scheme + "://" + nUrl.Host

	ext := ""
	if nUrl.Path != "" {
		trimmedPath := strings.TrimSuffix(nUrl.Path, "/")
		ext = filepath.Ext(nUrl.Path)
		normalizedURL += trimmedPath
	}

	return normalizedURL, ext, nil
}

func isUrlOfAllowedResourceType(ext string) bool {
	commonWebAndImgExtensions := []string{
		".html", ".htm", ".php", ".asp", ".aspx", ".jsp",
		".txt",
	}

	// 	".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
	// ".bmp", ".tiff", ".ico",

	for _, allowedExt := range commonWebAndImgExtensions {
		if ext == allowedExt {
			return true
		}
	}

	return false
}

func extractDomainFromUrl(urlString string) (string, error) {
	parsedUrl, err := url.Parse(urlString)
	if err != nil {
		return "", fmt.Errorf("invalid URL %s: %w", urlString, err)
	}

	if parsedUrl.Host == "" {
		return "", fmt.Errorf("URL %s does not contain a host", urlString)
	}

	return parsedUrl.Host, nil
}
