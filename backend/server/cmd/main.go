package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"server/internals/config"
	"server/internals/router"
	"server/internals/utils"
	"syscall"

	fiberZap "github.com/gofiber/contrib/v3/zap"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	dev := flag.Bool("dev", false, "Enable development environment behavior")
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := zap.Must(zap.NewProduction())
	if *dev {
		logger = zap.Must(zap.NewDevelopment())
	}

	ctx := context.Background()

	cfg, err := config.Load(*configPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debug("Config file not found")
		} else {
			logger.Fatal("Failed to load config", zap.Error(err))
		}
	}
	logger.Debug("Config loaded")

	logger, err = utils.NewConfiguredLogger(dev, cfg)
	if err != nil {
		logger.Fatal("Failed to configure logger", zap.Error(err))
	}

	err = godotenv.Load(*envPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debug("env file not found")
		} else {
			logger.Fatal("Failed to load env file", zap.Error(err))
		}
	}
	logger.Debug("env variables loaded")

	dbPool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		logger.Fatal("Failed to connect to PostgreSQL", zap.Error(err))
	}
	logger.Debug("PostgreSQL client initialized")
	defer dbPool.Close()

	app := fiber.New(fiber.Config{
		UnescapePath: true,
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, exiting...")
		err = app.Shutdown()
		if err != nil {
			logger.Error("Failed to cleanly shutdown http server", zap.Error(err))
		}
		os.Exit(0)
	}()

	app.Use(fiberZap.New(fiberZap.Config{
		Logger: logger,
	}))

	api := app.Group("/api/v1")
	router.SetupRoutes(api, dbPool)

	logger.Info("Starting...")

	err = app.Listen(fmt.Sprintf(":%s", cfg.Server.Port))
	if err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}
