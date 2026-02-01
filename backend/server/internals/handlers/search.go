package handlers

import (
	"server/internals/storage/database"
	"server/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
)

func GetQuery(queries *database.Queries) fiber.Handler {
	return func(c *fiber.Ctx) error {

		query := c.Params("query")
		params := c.Queries()
		querySplit := strings.Fields(query)
		filteredWords := utils.RemoveStopWords(querySplit)
		stemmedQuery := utils.StemWords(filteredWords)

		query_results, err := queries.GetSearchResults(c.Context(), stemmedQuery)
		if err != nil {
			return c.Status(fiber.StatusRequestTimeout).JSON(fiber.Map{
				"error": "Unable to get results",
			})
		}

		return c.JSON(fiber.Map{
			"result": query_results,
			"params": params,
		})
	}
}
