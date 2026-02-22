package router

import (
	// "crypto/sha256"
	// "crypto/subtle"
	// "os"
	// "server/client/templates"
	// "server/internals/handlers"
	// "crypto/sha256"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"os"
	"server/internals/handlers"
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/extractors"
	"github.com/gofiber/fiber/v3/middleware/keyauth"
	// "github.com/gofiber/fiber/v3/middleware/keyauth"
	// "github.com/gofiber/fiber/v3/middleware/keyauth"
)

func setupClient(app fiber.Router, searchService *services.SearchService, crawlerBoardService *services.CrawlerBoardService) {
	authMiddleware := keyauth.New(keyauth.Config{
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

	app.Get("/", handlers.ServeIndex())

	app.Get("/search", handlers.ServeResults(searchService))

	app.Get("/admin", authMiddleware, handlers.ServeAdmin())
	app.Post("/admin", authMiddleware, handlers.VerifyAdmin())

	crawlerBoardRouter(app.Group("/crawlerboard", authMiddleware), crawlerBoardService)
}

func crawlerBoardRouter(app fiber.Router, service *services.CrawlerBoardService) {
	app.Get("/", handlers.GetCrawlerboardPage(service))
	app.Post("/", handlers.AddUrlToCrawlerboard(service))
	app.Delete("/", handlers.RejectCrawlerboardPage(service))
	app.Post("/accept", handlers.AcceptSubmissions(service))
}
