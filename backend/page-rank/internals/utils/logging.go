package utils

import (
	"page-rank/internals/config"

	"go.uber.org/zap"
)

func NewConfiguredLogger(dev *bool, cfg *config.Config) (*zap.Logger, error) {
	var zapConfig zap.Config
	var level zap.AtomicLevel

	encoderConfig := zap.NewProductionEncoderConfig()
	if *dev {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
	}

	switch cfg.Logging.Level {
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "fatal":
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
	}

	zapConfig = zap.Config{
		Level:            level,
		Development:      *dev,
		OutputPaths:      cfg.Logging.OutputPaths,
		ErrorOutputPaths: cfg.Logging.OutputPaths,
		Encoding:         cfg.Logging.Encoding,
		EncoderConfig:    encoderConfig,
	}

	return zapConfig.Build()
}
