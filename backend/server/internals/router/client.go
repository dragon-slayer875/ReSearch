package router

import (
	// "crypto/sha256"
	// "crypto/subtle"
	// "os"
	// "server/client/templates"
	// "server/internals/handlers"
	// "crypto/sha256"
	"server/internals/handlers"
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	// "github.com/gofiber/fiber/v3/middleware/keyauth"
	// "github.com/gofiber/fiber/v3/middleware/keyauth"
)

func setupClient(app fiber.Router, linksService *services.SearchService, crawlerBoardService *services.CrawlerBoardService) {
	app.Get("/", handlers.ServeIndex())

	app.Get("/search", handlers.ServeResults(linksService))
	crawlerBoardRouter(app.Group("/crawlerboard"), crawlerBoardService)
}

func crawlerBoardRouter(app fiber.Router, service *services.CrawlerBoardService) {
	// authMiddleware := keyauth.New(keyauth.Config{ Validator: func(c fiber.Ctx, key string) (bool, error) {
	// 		hashedAPIKey := sha256.Sum256([]byte(os.Getenv("ADMIN_KEY")))
	// 		hashedKey := sha256.Sum256([]byte(key))
	//
	// 		if subtle.ConstantTimeCompare(hashedAPIKey[:], hashedKey[:]) == 1 {
	// 			return true, nil
	// 		}
	// 		return false, keyauth.ErrMissingOrMalformedAPIKey
	// 	},
	// })

	app.Get("/", handlers.GetCrawlerboardPage(service))
	app.Post("/", handlers.AddUrlToCrawlerboard(service))
	app.Delete("/", handlers.RejectCrawlerboardPage(service))
	// app.Post("/submissions", handlers.PostSubmissions(service))
	// app.Post("/submissions/accept", authMiddleware, handlers.AcceptSubmissions(service))
	// app.Post("/submissions/reject", authMiddleware, handlers.RejectSubmissions(service))
}
