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

		query_results, err := service.GetSearchResults(c.Context(), req)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(query_results)
	}
}
