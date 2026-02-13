package router

import (
	"crypto/sha256"
	"crypto/subtle"
	"os"
	"server/internals/handlers"
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/keyauth"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func SetupRoutes(app fiber.Router, dbPool *pgxpool.Pool, redisClient *redis.Client) {
	searchService := services.NewSearchService(dbPool)
	crawlerBoardService := services.NewCrawlerBoardService(redisClient)

	setupClient(app, searchService, crawlerBoardService)
	setupApiRoutes(app.Group("/api/v1"), searchService, crawlerBoardService)
}

func setupApiRoutes(app fiber.Router, linksService *services.SearchService, crawlerBoardService *services.CrawlerBoardService) {
	apiSearchRouter(app.Group("/search"), linksService)
	apiCrawlerBoardRouter(app.Group("/crawlerboard"), crawlerBoardService)
}

func apiSearchRouter(app fiber.Router, service *services.SearchService) {
	app.Get("/:query", handlers.GetQuery(service))
}

func apiCrawlerBoardRouter(app fiber.Router, service *services.CrawlerBoardService) {
	authMiddleware := keyauth.New(keyauth.Config{
		Validator: func(c fiber.Ctx, key string) (bool, error) {
			hashedAPIKey := sha256.Sum256([]byte(os.Getenv("ADMIN_KEY")))
			hashedKey := sha256.Sum256([]byte(key))

			if subtle.ConstantTimeCompare(hashedAPIKey[:], hashedKey[:]) == 1 {
				return true, nil
			}
			return false, keyauth.ErrMissingOrMalformedAPIKey
		},
	})

	app.Get("/submissions", handlers.GetSubmissions(service))
	app.Post("/submissions", handlers.PostSubmissions(service))
	app.Post("/submissions/accept", authMiddleware, handlers.AcceptSubmissions(service))
	app.Post("/submissions/reject", authMiddleware, handlers.RejectSubmissions(service))
}
