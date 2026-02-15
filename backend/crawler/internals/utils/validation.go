package utils

import (
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
)

func NormalizeURL(parentUrl, newUrl string) (string, string, bool, error) {
	allowed := false
	pUrl, err := url.Parse(parentUrl)
	if parentUrl != "" && err != nil {
		return "", "", allowed, fmt.Errorf("could not parse parent URL: [%w]", err)
	}

	nUrl, err := url.Parse(newUrl)
	if err != nil {
		return "", "", allowed, fmt.Errorf("could not parse URL to normalize: [%w]", err)
	}

	nUrl = pUrl.ResolveReference(nUrl)

	if nUrl.Scheme != "https" && nUrl.Scheme != "http" && nUrl.Scheme != "" {
		return "", "", allowed, fmt.Errorf("url has invalid 'Scheme'")
	}

	if nUrl.Host == "" {
		return "", "", allowed, fmt.Errorf("url has no 'Host'")
	}

	domain := nUrl.Host

	if nUrl.Scheme == "" {
		nUrl.Scheme = "https"
	}

	ext := ""
	allowed = true

	if nUrl.Path != "" {
		ext = filepath.Ext(nUrl.Path)
	}

	if ext != "" {
		allowed = isUrlOfAllowedResourceType(ext)
	}

	return nUrl.Scheme + "://" + domain + nUrl.Path, domain, allowed, nil
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
