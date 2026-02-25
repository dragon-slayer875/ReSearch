package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	TfIdf   TfIdfConfig   `mapstructure:"tf_idf"`
	Logging LoggingConfig `mapstructure:"logging"`
	Retryer RetryerConfig `mapstructure:"retryer"`
}

type TfIdfConfig struct {
	Interval     string `mapstructure:"interval"`
	LockDuration string `mapstructure:"lock_duration"`
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

	viper.SetConfigFile(configPath)
	configReadingErr = viper.ReadInConfig()

	viper.SetDefault("tf_idf.interval", "30m")
	viper.SetDefault("tf_idf.lock_duration", "30m")
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
