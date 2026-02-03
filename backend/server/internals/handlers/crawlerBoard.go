package handlers

import (
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

func GetSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		submissions, err := service.GetSubmissions(c.Context())
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(submissions)
	}
}

func PostSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body struct {
			Entries []string `json:"entries"`
		}

		err := c.Bind().Body(&body)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		c.SendString("Entries Added")
		return service.AddSubmissions(c.Context(), body.Entries)
	}
}

type voteRequest struct {
	Submissions []string `json:"submissions"`
	Vote        string   `json:"vote"`
}

func PostVotes(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body voteRequest

		err := c.Bind().Body(&body)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		if body.Vote != "up" && body.Vote != "down" {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest, "Unknown vote type")
		}

		nonExistentSubmissions, err := service.UpdateVotes(c.Context(), body.Submissions, body.Vote)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(nonExistentSubmissions)
	}
}
