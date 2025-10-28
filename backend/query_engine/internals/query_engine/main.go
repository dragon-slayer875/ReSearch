package queryEngine

import (
	"context"
	"log"
	"query_engine/internals/storage/database"
	"query_engine/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QueryEngine struct {
	restServer *fiber.App
	logger     *log.Logger
	dbPool     *pgxpool.Pool
	ctx        context.Context
}

type QueryEngineImpl interface {
	Start(port string) error
	processQuery(query string) string
	retrieveDocuments(queryStrings []string) string
}

func NewQueryEngine(restServer *fiber.App, logger *log.Logger, dbPool *pgxpool.Pool, ctx context.Context) *QueryEngine {
	return &QueryEngine{
		restServer,
		logger,
		dbPool,
		ctx,
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

	documents, err := qe.retrieveDocuments(stemmed_query)
	resString := ""

	if err != nil {
		qe.logger.Println(err)
		return ""
	}

	for key, val := range documents {
		resString += " " + key
		qe.logger.Println(val)
	}

	return resString
}

func (qe *QueryEngine) retrieveDocuments(queryStrings []string) (map[string]database.BatchGetInvertedIndexByWordRow, error) {
	queries := database.New(qe.dbPool)
	documents := make(map[string]database.BatchGetInvertedIndexByWordRow)
	var getErr error
	query_results := queries.BatchGetInvertedIndexByWord(qe.ctx, queryStrings)
	query_results.QueryRow(func(i int, ii database.BatchGetInvertedIndexByWordRow, err error) {
		if err != nil {
			getErr = err
			return
		}
		documents[ii.Word] = ii
	})

	return documents, getErr
}
