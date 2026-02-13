package services

import (
	"context"
	"math"
	"server/internals/storage/database"
	"server/internals/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SearchService struct {
	queries *database.Queries
}

func NewSearchService(pool *pgxpool.Pool) *SearchService {
	return &SearchService{
		database.New(pool),
	}
}

func (l *SearchService) GetSearchResults(ctx context.Context, req *utils.SearchGetRequest) (*utils.SearchGetResponse, error) {
	cleanedQuery := utils.CleanQuery(req.Query)

	count, err := l.queries.GetSearchResultCount(ctx, *cleanedQuery)
	if err != nil {
		return nil, err
	}

	results, err := l.queries.GetSearchResults(ctx, database.GetSearchResultsParams{
		Column1: *cleanedQuery,
		Limit:   req.Limit,
		Offset:  (req.Page - 1) * req.Limit,
	})
	if err != nil {
		return nil, err
	}

	return &utils.SearchGetResponse{
		Pages:   int(math.Ceil(float64(count) / float64(req.Limit))),
		Count:   count,
		Results: results,
	}, nil
}
