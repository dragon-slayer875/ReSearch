package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Server  serverConfig  `mapstructure:"server"`
	Logging loggingConfig `mapstructure:"logging"`
}

type serverConfig struct {
	Port string `mapstructure:"port"`
}

type loggingConfig struct {
	Level       string   `mapstructure:"level"`
	Encoding    string   `mapstructure:"encoding"`
	OutputPaths []string `mapstructure:"output_paths"`
}

func Load(configPath string) (*Config, error) {
	var configReadingErr error

	viper.SetConfigFile(configPath)
	configReadingErr = viper.ReadInConfig()

	viper.SetDefault("server.port", "3001")
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.encoding", "json")
	viper.SetDefault("logging.output_paths", []string{"stderr"})

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, configReadingErr
}
