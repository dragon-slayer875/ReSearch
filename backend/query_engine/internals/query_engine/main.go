package queryEngine

import (
	"context"
	"query_engine/internals/storage/database"
	"query_engine/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type QueryEngine struct {
	restServer *fiber.App
	logger     *zap.Logger
	dbPool     *pgxpool.Pool
	ctx        context.Context
}

type QueryEngineImpl interface {
	Start(port string) error
	processQuery(query string) []database.GetSearchResultsRow
	retrieveDocuments(queryStrings []string) ([]database.GetSearchResultsRow, error)
}

func NewQueryEngine(restServer *fiber.App, logger *zap.Logger, dbPool *pgxpool.Pool, ctx context.Context) *QueryEngine {
	return &QueryEngine{
		restServer,
		logger,
		dbPool,
		ctx,
	}
}

func (qe *QueryEngine) Start(port string) error {
	qe.restServer.Get("/api/search/:search", func(c *fiber.Ctx) error {
		return c.JSON(qe.processQuery(c.Params("search")))
	})

	return qe.restServer.Listen(port)
}

func (qe *QueryEngine) processQuery(query string) []database.GetSearchResultsRow {
	query_split := strings.Fields(query)
	filtered_words := utils.RemoveStopWords(query_split)
	stemmed_query := utils.StemWords(filtered_words)

	documents, err := qe.retrieveDocuments(stemmed_query)

	if err != nil {
		qe.logger.Error("Error retriving query results", zap.String("query", query), zap.Error(err))
		return nil
	}

	if len(documents) == 0 {
		return nil
	}

	qe.logger.Info("Processed query", zap.Int("results", len(documents)))

	return documents
}

func (qe *QueryEngine) retrieveDocuments(queryStrings []string) ([]database.GetSearchResultsRow, error) {
	queries := database.New(qe.dbPool)
	query_results, err := queries.GetSearchResults(qe.ctx, queryStrings)

	return query_results, err
}
