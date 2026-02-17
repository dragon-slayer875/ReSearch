package handlers

import (
	"server/internals/services"
	"server/internals/utils"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

func GetQuery(service *services.SearchService) fiber.Handler {
	return func(c fiber.Ctx) error {
		req := new(utils.SearchGetRequest)

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		isFirstPage := req.Page == 1

		contentWords, _, wordsAndSuggestions, err := service.GetSuggestions(c.Context(), req.Query)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		query_results, totalPages, err := service.GetSearchResults(c.Context(), contentWords, req.Limit, req.Page, isFirstPage)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		go func() {
			err = service.CacheQueryData(c.Context(), contentWords, query_results, totalPages, wordsAndSuggestions, isFirstPage)
			if err != nil {
				log.Error(err)
			}
		}()

		return c.JSON(map[string]any{
			"query results": query_results,
			"total pages":   totalPages,
		})
	}
}
