package utils

import (
	"net"
	"time"

	redisLib "github.com/redis/go-redis/v9"
)

type WebPage struct {
	Url                string
	Domain             string
	Outlinks           *[]string
	DomainQueueMembers *[]redisLib.Z
	DomainAndUrls      *map[string][]any
	HtmlContent        *[]byte
	HttpStatusCode     int
}

func IsInternetAvailable() bool {
	endpoints := []string{
		"1.1.1.1:53",
		"8.8.8.8:53",
	}

	timeout := 3 * time.Second

	for _, endpoint := range endpoints {
		conn, err := net.DialTimeout("tcp", endpoint, timeout)
		if err == nil {
			conn.Close()
			return true
		}
	}

	return false
}
