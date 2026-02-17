package utils

import (
	"server/internals/storage/database"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
)

type ResultsPageData struct {
	SearchResults *[]database.GetSearchResultsRow
	TotalPages    int64
	CurrentPage   int64
	Suggestion    string
	Query         string
}

func Render(c fiber.Ctx, component templ.Component) error {
	c.Set("Content-Type", "text/html")
	return component.Render(c.Context(), c.Response().BodyWriter())
}

func CalculatePages(totalPages, currentPage int64) (int64, int64) {
	page := int64(1)
	maxPages := totalPages

	if totalPages > 10 {
		if currentPage <= 5 {
			maxPages = 10
		} else {
			page = currentPage - 4
			maxPages = currentPage + 5

			if maxPages > totalPages {
				maxPages = totalPages
				page = maxPages - 9
			}
		}
	}

	return page, maxPages
}
