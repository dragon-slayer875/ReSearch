package services

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"server/internals/storage/database"
)

type SearchService struct {
	queries *database.Queries
}

func NewSearchService(pool *pgxpool.Pool) *SearchService {
	return &SearchService{
		database.New(pool),
	}
}

func (l *SearchService) GetSearchResults(ctx context.Context, query *[]string, page, limit int32) ([]database.GetSearchResultsRow, error) {
	return l.queries.GetSearchResults(ctx, database.GetSearchResultsParams{
		Column1: *query,
		Limit:   limit,
		Offset:  page - 1,
	})
}
