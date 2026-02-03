package handlers

import (
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

type submissionRequest struct {
	Submissions []string `json:"submissions"`
}

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
		var body submissionRequest

		if err := c.Bind().Body(&body); err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		c.SendString("Entries Added")
		return service.AddSubmissions(c.Context(), body.Submissions)
	}
}

type voteRequest struct {
	Submissions []string `json:"submissions"`
	Vote        string   `json:"vote"`
}

func Vote(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body voteRequest

		if err := c.Bind().Body(&body); err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		if body.Vote != "up" && body.Vote != "down" {
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

func AcceptSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body submissionRequest

		if err := c.Bind().Body(&body); err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		nonExistentSubmissions, err := service.AcceptSubmissions(c.Context(), body.Submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(nonExistentSubmissions)
	}
}

func RejectSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body submissionRequest

		if err := c.Bind().Body(&body); err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		nonExistentSubmissions, err := service.RejectSubmissions(c.Context(), body.Submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(nonExistentSubmissions)
	}
}
