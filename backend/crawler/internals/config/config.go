package config

import (
	"github.com/spf13/viper"
)

// Config holds the configuration for the crawler
type Config struct {
	Crawler CrawlerConfig `mapstructure:"crawler"`
	Logging LoggingConfig `mapstructure:"logging"`
	Retryer RetryerConfig `mapstructure:"retryer"`
}

type CrawlerConfig struct {
	WorkerCount     int    `mapstructure:"worker_count"`
	HTTPTimeout     int    `mapstructure:"http_timeout"` // in seconds
	MaxIdleConns    int    `mapstructure:"max_idle_conns"`
	MaxConnsPerHost int    `mapstructure:"max_conns_per_host"`
	IdleConnTimeout int    `mapstructure:"idle_conn_timeout"` // in seconds
	UserAgent       string `mapstructure:"user_agent"`
}

type LoggingConfig struct {
	Level       string   `mapstructure:"level"`
	Encoding    string   `mapstructure:"encoding"`
	OutputPaths []string `mapstructure:"output_paths"`
}

type RetryerConfig struct {
	MaxRetries        int     `mapstructure:"max_retries"`
	InitialBackoff    string  `mapstructure:"initial_backoff"`
	MaxBackoff        string  `mapstructure:"max_backoff"`
	BackoffMultiplier float64 `mapstructure:"backoff_multiplier"`
}

func Load(configPath string) (*Config, error) {
	var configReadingErr error

	if configPath != "" {
		viper.SetConfigFile(configPath)
		configReadingErr = viper.ReadInConfig()
	}

	viper.SetDefault("crawler.worker_count", 5)
	viper.SetDefault("crawler.http_timeout", 0) // in seconds
	viper.SetDefault("crawler.max_idle_conns", 100)
	viper.SetDefault("crawler.max_conns_per_host", 1)
	viper.SetDefault("crawler.idle_conn_timeout", 0)             // in seconds
	viper.SetDefault("crawler.user_agent", "Go-http-client/1.1") // default user agent
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.encoding", "json")
	viper.SetDefault("logging.output_paths", []string{"stderr"})
	viper.SetDefault("retryer.max_retries", 5)
	viper.SetDefault("retryer.initial_backoff", "500ms") // pattern is <number><unit> ns, us, ms, s, m, h
	viper.SetDefault("retryer.max_backoff", "30s")
	viper.SetDefault("retryer.backoff_multiplier", 2)

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, configReadingErr
}
