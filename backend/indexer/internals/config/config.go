package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Indexer IndexerConfig `mapstructure:"indexer"`
	Logging LoggingConfig `mapstructure:"logging"`
}

type IndexerConfig struct {
	WorkerCount int `mapstructure:"worker_count"`
}

type LoggingConfig struct {
	Level       string   `mapstructure:"level"`
	Encoding    string   `mapstructure:"encoding"`
	OutputPaths []string `mapstructure:"output_paths"`
}

func Load(configPath string) (*Config, error) {
	var configReadingErr error

	viper.SetConfigFile(configPath)
	configReadingErr = viper.ReadInConfig()

	viper.SetDefault("indexer.worker_count", 5)
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.encoding", "json")
	viper.SetDefault("logging.output_paths", []string{"stderr"})

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, configReadingErr
}
