package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"tf-idf/internals/storage/database"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	envPath := flag.String("env", ".env", "Path to env variables file")
	flag.Parse()

	logger := log.New(os.Stdout, "tf-idf: ", log.LstdFlags)

	ctx := context.Background()

	err := godotenv.Load(*envPath)
	if err != nil {
		logger.Fatalln("Error loading environment variables:", err)
	}

	dbPool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		logger.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	defer dbPool.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		logger.Println("Received shutdown signal, exiting...")
		os.Exit(0)
	}()

	logger.Println("Starting...")

	queries := database.New(dbPool)
	for {
		time.Sleep(10 * time.Minute)
		logger.Println("Updating tf-idf")

		err := queries.UpdateTfIdf(ctx)
		if err != nil {
			logger.Println(err)
			continue
		}

		logger.Println("tf-idf update completed")
	}
}
