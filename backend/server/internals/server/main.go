package server

import (
	"context"
	"server/internals/storage/database"
	"server/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Server struct {
	restServer *fiber.App
	logger     *zap.Logger
	dbPool     *pgxpool.Pool
	ctx        context.Context
}

func NewServer(restServer *fiber.App, logger *zap.Logger, dbPool *pgxpool.Pool, ctx context.Context) *Server {
	return &Server{
		restServer,
		logger,
		dbPool,
		ctx,
	}
}

func (s *Server) Start(port string) error {
	api := s.restServer.Group("/api")
	v1 := api.Group("/v1")

	v1.Get("/search/:query", func(c *fiber.Ctx) error {
		return c.JSON(s.processQuery(c.Params("query")))
	})

	return s.restServer.Listen(port)
}

func (s *Server) processQuery(query string) []database.GetSearchResultsRow {
	query_split := strings.Fields(query)
	filtered_words := utils.RemoveStopWords(query_split)
	stemmed_query := utils.StemWords(filtered_words)

	documents, err := s.retrieveDocuments(stemmed_query)

	if err != nil {
		s.logger.Error("Error retriving query results", zap.String("query", query), zap.Error(err))
		return nil
	}

	s.logger.Info("Processed query", zap.Int("results", len(documents)))

	return documents
}

func (s *Server) retrieveDocuments(queryStrings []string) ([]database.GetSearchResultsRow, error) {
	queries := database.New(s.dbPool)
	query_results, err := queries.GetSearchResults(s.ctx, queryStrings)

	return query_results, err
}
