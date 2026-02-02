package handlers

import (
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
)

func GetQuery(service *services.LinksService) fiber.Handler {
	return func(c fiber.Ctx) error {
		query := c.Params("query")
		params := c.Queries()

		query_results, err := service.GetLinks(c.Context(), query)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(fiber.Map{
			"result": query_results,
			"params": params,
		})
	}
}
