package router

import (
	"server/internals/handlers"
	"server/internals/storage/database"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func SetupRoutes(app fiber.Router, dbPool *pgxpool.Pool) {
	searchRouter(app.Group("/search"), dbPool)
}

func searchRouter(app fiber.Router, dbPool *pgxpool.Pool) {
	app.Get("/:query", handlers.GetQuery(database.New(dbPool)))
}
