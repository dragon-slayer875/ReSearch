package router

import (
	"server/internals/handlers"
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func SetupRoutes(app fiber.Router, dbPool *pgxpool.Pool, redisClient *redis.Client) {
	linksService := services.NewLinksService(dbPool)
	crawlerBoardService := services.NewCrawlerBoardService(redisClient)

	searchRouter(app.Group("/search"), linksService)
	crawlerBoardRouter(app.Group("/crawlerboard"), crawlerBoardService)
}

func searchRouter(app fiber.Router, service *services.LinksService) {
	app.Get("/:query", handlers.GetQuery(service))
}

func crawlerBoardRouter(app fiber.Router, service *services.CrawlerBoardService) {
	app.Get("/submissions", handlers.GetSubmissions(service))
	app.Post("/submissions", handlers.PostSubmissions(service))
	app.Post("/submissions/votes", handlers.Vote(service))
	app.Post("/submissions/accept", handlers.AcceptSubmissions(service))
	app.Post("/submissions/reject", handlers.RejectSubmissions(service))
}
