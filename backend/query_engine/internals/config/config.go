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
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()

	viper.SetDefault("query_engine.port", "3001")
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
