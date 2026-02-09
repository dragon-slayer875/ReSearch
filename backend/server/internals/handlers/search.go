package handlers

import (
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

type searchGetRequest struct {
	// Sort  string `query:"sort,default:score" validate:"oneof=score time"`
	// Order string `query:"order,default:dsc" validate:"oneof=asc dsc"`
	Page  int    `query:"page,default:1" validate:"gt=0"`
	Query string `query:"query"`
}

func GetQuery(service *services.LinksService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req searchGetRequest

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		query_results, err := service.GetLinks(c.Context(), req.Query)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(fiber.Map{
			"result": query_results,
			"params": req,
		})
	}
}
