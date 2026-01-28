package utils

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

func NormalizeURL(parentUrl, newUrl string) (string, string, string, error) {
	pUrl, err := url.Parse(parentUrl)
	if parentUrl != "" && err != nil {
		return "", "", "", fmt.Errorf("could not parse parent URL: [%w]", err)
	}

	nUrl, err := url.Parse(newUrl)
	if err != nil {
		return "", "", "", fmt.Errorf("could not parse URL to normalize: [%w]", err)
	}

	// If the URL is relative, resolve it against the parent URL
	nUrl = pUrl.ResolveReference(nUrl)

	if nUrl.Scheme != "https" && nUrl.Scheme != "http" && nUrl.Scheme != "" {
		return "", "", "", fmt.Errorf("url has invalid 'Scheme'")
	}

	if nUrl.Host == "" {
		return "", "", "", fmt.Errorf("url has no 'Host'")
	}

	var normalizedURL string
	if nUrl.Scheme == "" {
		normalizedURL = "http" + "://" + nUrl.Host
	} else {
		normalizedURL = nUrl.Scheme + "://" + nUrl.Host
	}

	ext := ""
	if nUrl.Path != "" {
		trimmedPath := strings.TrimSuffix(nUrl.Path, "/")
		ext = filepath.Ext(nUrl.Path)
		normalizedURL += trimmedPath
	}

	return normalizedURL, nUrl.Hostname(), ext, nil
}

func IsUrlOfAllowedResourceType(ext string) bool {
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
