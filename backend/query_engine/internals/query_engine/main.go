package queryEngine

import "github.com/gofiber/fiber/v2"

type QueryEngine struct {
	RestServer *fiber.App
}

type QueryEngineImpl interface {
	Start(port string) error
}

func NewQueryEngine(restServer *fiber.App) *QueryEngine {
	return &QueryEngine{
		RestServer: restServer,
	}
}

func (qe *QueryEngine) Start(port string) error {
	qe.RestServer.Get("/api/:search", func(c *fiber.Ctx) error {
		return c.SendString(c.Params("search"))
	})

	return qe.RestServer.Listen(port)
}
