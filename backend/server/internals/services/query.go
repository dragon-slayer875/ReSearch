package services

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"server/internals/storage/database"
	"server/internals/utils"
	"strings"
)

type LinksService struct {
	queries *database.Queries
}

func NewLinksService(pool *pgxpool.Pool) *LinksService {
	return &LinksService{
		database.New(pool),
	}
}

func (l *LinksService) GetLinks(ctx context.Context, query string) ([]database.GetSearchResultsRow, error) {
	querySplit := strings.Fields(query)
	filteredWords := utils.RemoveStopWords(querySplit)
	stemmedQuery := utils.StemWords(filteredWords)

	return l.queries.GetSearchResults(ctx, stemmedQuery)

}
