package router

import (
	"server/internals/handlers"
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
)

func SetupRoutes(app fiber.Router, dbPool *pgxpool.Pool) {
	linksService := services.NewLinksService(dbPool)

	searchRouter(app.Group("/search"), linksService)
}

func searchRouter(app fiber.Router, service *services.LinksService) {
	app.Get("/:query", handlers.GetQuery(service))
}
