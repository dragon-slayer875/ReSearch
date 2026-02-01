package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	QueryEngine QueryEngineConfig `mapstructure:"query_engine"`
	Logging     LoggingConfig     `mapstructure:"logging"`
}

type QueryEngineConfig struct {
	Port string `mapstructure:"port"`
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

	viper.SetDefault("query_engine.port", "3001")
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.encoding", "json")
	viper.SetDefault("logging.output_paths", []string{"stderr"})

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, configReadingErr
}
