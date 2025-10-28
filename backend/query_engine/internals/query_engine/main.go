package queryEngine

import (
	"log"
	"query_engine/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QueryEngine struct {
	restServer *fiber.App
	logger     *log.Logger
	dbPool     *pgxpool.Pool
}

type QueryEngineImpl interface {
	Start(port string) error
	processQuery(query string) string
	retrieveDocuments(queryStrings []string) string
}

func NewQueryEngine(restServer *fiber.App, logger *log.Logger, dbPool *pgxpool.Pool) *QueryEngine {
	return &QueryEngine{
		restServer,
		logger,
		dbPool,
	}
}

func (qe *QueryEngine) Start(port string) error {
	qe.restServer.Get("/api/:search", func(c *fiber.Ctx) error {
		return c.SendString(qe.processQuery(c.Params("search")))
	})

	return qe.restServer.Listen(port)
}

func (qe *QueryEngine) processQuery(query string) string {
	query_split := strings.Fields(query)
	filtered_words := utils.RemoveStopWords(query_split)
	stemmed_query := utils.StemWords(filtered_words)

	// documents := qe.retrieveDocuments(stemmed_query)

	return strings.Join(stemmed_query, " ")
}

func (qe *QueryEngine) retrieveDocuments(queryStrings []string) string {
	return ""
}
