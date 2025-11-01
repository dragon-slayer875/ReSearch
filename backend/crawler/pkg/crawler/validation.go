package crawler

import (
	"fmt"
	"net/url"
	"strings"
)

func validateUrl(parentUrl, dirtyUrl string) (string, error) {
	parsedParentUrl, err := url.Parse(parentUrl)
	if err != nil {
		return "", fmt.Errorf("invalid parent URL %s: %w", parentUrl, err)
	}

	parsedDirtyUrl, err := url.Parse(dirtyUrl)
	if err != nil {
		return "", fmt.Errorf("invalid URL %s: %w", dirtyUrl, err)
	}

	// If the URL is absolute, return it as is
	if parsedDirtyUrl.IsAbs() {
		return parsedDirtyUrl.String(), nil
	}

	// If the URL is relative, resolve it against the parent URL
	parsedDirtyUrl = parsedParentUrl.ResolveReference(parsedDirtyUrl)

	// Validate the resolved URL
	if parsedDirtyUrl.Scheme == "" || parsedDirtyUrl.Host == "" {
		return "", fmt.Errorf("resolved URL %s is invalid", parsedDirtyUrl.String())
	}

	// Check if the URL is valid and well-formed
	if parsedDirtyUrl.Scheme != "http" && parsedDirtyUrl.Scheme != "https" {
		return "", fmt.Errorf("URL %s must use http or https scheme", parsedDirtyUrl.String())
	}

	// Ensure the URL leads to only web pages or images
	if parsedDirtyUrl.Path != "" && strings.HasSuffix(parsedDirtyUrl.Path, "") {
	}

	return parsedDirtyUrl.String(), nil
}

func isUrlOfAllowedResourceType(urlString string) (bool, error) {
	commonWebAndImgExtensions := []string{
		".html", ".htm", ".php", ".asp", ".aspx", ".jsp",
		".txt",
	}

	// 	".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
	// ".bmp", ".tiff", ".ico",

	for _, ext := range commonWebAndImgExtensions {
		if strings.HasSuffix(urlString, ext) {
			return true, nil
		}
	}

	return false, nil
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
