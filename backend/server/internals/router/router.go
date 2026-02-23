package router

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"os"
	"server/internals/handlers"
	"server/internals/services"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/extractors"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/idempotency"
	"github.com/gofiber/fiber/v3/middleware/keyauth"
	"github.com/gofiber/fiber/v3/middleware/limiter"
	"github.com/gofiber/fiber/v3/middleware/static"
)

func authMiddleware() fiber.Handler {
	return keyauth.New(keyauth.Config{
		Validator: func(c fiber.Ctx, key string) (bool, error) {
			hashedAPIKey := sha256.Sum256([]byte(os.Getenv("ADMIN_KEY")))
			hashedKey := sha256.Sum256([]byte(key))

			if subtle.ConstantTimeCompare(hashedAPIKey[:], hashedKey[:]) == 1 {
				return true, nil
			}

			return false, keyauth.ErrMissingOrMalformedAPIKey
		},
		SuccessHandler: func(c fiber.Ctx) error {
			c.Locals("auth", true)
			return c.Next()
		},
		ErrorHandler: func(c fiber.Ctx, _ error) error {
			c.Locals("auth", false)
			return c.Next()
		},
		Extractor: extractors.Chain(
			extractors.FromCustom("key-from-form", func(c fiber.Ctx) (string, error) {
				key := c.FormValue("rs_key", "")
				if key == "" {
					return "", errors.New("value not found")
				}
				return key, nil
			}),
			extractors.FromCookie("rs_key"),
			extractors.FromAuthHeader("Bearer"),
		),
	})
}

func adminRouter(app fiber.Router) {
	app.Get("/", handlers.ServeAdmin())
	app.Post("/", handlers.VerifyAdmin())
}

func crawlerBoardRouter(app fiber.Router, service *services.CrawlerBoardService) {
	app.Get("/", handlers.CrawlerboardGet(service))
	app.Post("/", handlers.CrawlerboardAdd(service))
	app.Delete("/", handlers.CrawlerboardReject(service))
	app.Post("/accept", handlers.CrawlerboardAccept(service))
}

func SetupRoutes(app fiber.Router, dbPool *pgxpool.Pool, redisClient *redis.Client) {
	searchService := services.NewSearchService(dbPool, redisClient)
	crawlerBoardService := services.NewCrawlerBoardService(redisClient)

	app.Use(cors.New())

	app.Use(limiter.New(limiter.Config{
		Max:                   20,
		Expiration:            30 * time.Second,
		LimiterMiddleware:     limiter.SlidingWindow{},
		DisableValueRedaction: false, // Request keys are redacted by default to prevent leakage, set to true if needed for troubleshooting
	}))

	app.Use(idempotency.New()) // Default config skips safe methods: GET, HEAD, OPTIONS, TRACE

	app.Use("/", static.New("public"))

	app.Get("/", handlers.ServeIndex())

	app.Get("/search", handlers.ServeResults(searchService))

	adminRouter(app.Group("/admin", authMiddleware()))

	crawlerBoardRouter(app.Group("/crawlerboard", authMiddleware()), crawlerBoardService)
}
