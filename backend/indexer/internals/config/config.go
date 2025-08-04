package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Indexer CrawlerConfig `mapstructure:"indexer"`
	Logging LoggingConfig `mapstructure:"logging"`
}

type CrawlerConfig struct {
	WorkerCount int `mapstructure:"worker_count"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()

	viper.SetDefault("indexer.worker_count", 200)
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
