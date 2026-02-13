package handlers

import (
	"server/internals/services"
	"server/internals/utils"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

type searchGetRequest struct {
	// Sort  string `query:"sort,default:score" validate:"oneof=score time"`
	// Order string `query:"order,default:dsc" validate:"oneof=asc dsc"`
	Limit int32  `query:"limit,default:10" validate:"gt=0"`
	Page  int32  `query:"page,default:1" validate:"gt=0"`
	Query string `query:"query"`
}

func GetQuery(service *services.SearchService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req searchGetRequest

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		cleanedQuery := utils.CleanQuery(req.Query)

		query_results, err := service.GetSearchResults(c.Context(), cleanedQuery, req.Page, req.Limit)
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
