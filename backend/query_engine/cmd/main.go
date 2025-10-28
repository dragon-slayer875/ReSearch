package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"query_engine/internals/config"
	queryEngine "query_engine/internals/query_engine"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := log.New(os.Stdout, "queryEngine: ", log.LstdFlags)

	err := godotenv.Load(*envPath)
	if err != nil {
		logger.Fatalln("Error loading environment variables:", err)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatalln("Failed to load config:", err)
	}

	app := fiber.New(fiber.Config{
		UnescapePath: true,
	})

	QueryEngine := queryEngine.NewQueryEngine(app)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Println("Received shutdown signal, exiting...")
		err = app.Shutdown()
		if err != nil {
			logger.Println(err)
		}
		os.Exit(0)
	}()

	logger.Println("Starting...")
	err = QueryEngine.Start(fmt.Sprintf(":%s", cfg.QueryEngine.Port))
	if err != nil {
		logger.Fatalln(err)
	}
}
