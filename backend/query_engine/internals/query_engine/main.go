package queryEngine

import (
	"query_engine/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
)

type QueryEngine struct {
	RestServer *fiber.App
}

type QueryEngineImpl interface {
	Start(port string) error
	ProcessQuery(query string) string
}

func NewQueryEngine(restServer *fiber.App) *QueryEngine {
	return &QueryEngine{
		RestServer: restServer,
	}
}

func (qe *QueryEngine) Start(port string) error {
	qe.RestServer.Get("/api/:search", func(c *fiber.Ctx) error {
		return c.SendString(qe.ProcessQuery(c.Params("search")))
	})

	return qe.RestServer.Listen(port)
}

func (qe *QueryEngine) ProcessQuery(query string) string {
	query_split := strings.Split(query, " ")
	result := utils.StemWords(query_split)
	return strings.Join(result, " ")
}
