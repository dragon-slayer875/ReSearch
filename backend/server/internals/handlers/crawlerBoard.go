package handlers

import (
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

type submissionRequest struct {
	Submissions []string `json:"submissions" validate:"required,gt=0"`
}

type voteRequest struct {
	Submissions []string `json:"submissions" validate:"required,gt=0"`
	Vote        string   `json:"vote" validate:"required,oneof=up down"`
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
			return fiber.NewError(fiber.StatusBadRequest)
		}

		c.Status(fiber.StatusCreated)
		failedSubmissions, err := service.AddSubmissions(c.Context(), body.Submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(failedSubmissions)
	}
}

func Vote(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body voteRequest

		if err := c.Bind().Body(&body); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
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
