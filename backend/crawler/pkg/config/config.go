package config

import (
	"github.com/spf13/viper"
)

// Config holds the configuration for the crawler
type Config struct {
	Crawler CrawlerConfig `mapstructure:"crawler"`
	Logging LoggingConfig `mapstructure:"logging"`
}

type CrawlerConfig struct {
	WorkerCount     int `mapstructure:"worker_count"`
	HTTPTimeout     int `mapstructure:"http_timeout"` // in seconds
	MaxIdleConns    int `mapstructure:"max_idle_conns"`
	MaxConnsPerHost int `mapstructure:"max_conns_per_host"`
	IdleConnTimeout int `mapstructure:"idle_conn_timeout"` // in seconds
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()

	viper.SetDefault("crawler.worker_count", 200)
	viper.SetDefault("crawler.http_timeout", 0) // in seconds
	viper.SetDefault("crawler.max_idle_conns", 100)
	viper.SetDefault("crawler.max_conns_per_host", 1)
	viper.SetDefault("crawler.idle_conn_timeout", 0) // in seconds
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
