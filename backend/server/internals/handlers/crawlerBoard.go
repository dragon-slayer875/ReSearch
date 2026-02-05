package handlers

import (
	"server/internals/services"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

type submissionPostRequest struct {
	Submissions []string `json:"submissions" validate:"required,gt=0"`
}

type submissionGetRequest struct {
	Sort  string `query:"sort,default:score" validate:"oneof=score time"`
	Order string `query:"order,default:dsc" validate:"oneof=asc dsc"`
	Page  int    `query:"page,default:1" validate:"gt=0"`
	Limit int    `query:"page,default:10" validate:"gt=0"`
}

type voteRequest struct {
	Submissions []string `json:"submissions" validate:"required,gt=0"`
	Vote        string   `json:"vote" validate:"required,oneof=up down"`
}

func GetSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req submissionGetRequest

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		submissions, err := service.GetSubmissions(c.Context(), req.Sort, req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(submissions)
	}
}

func PostSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body submissionPostRequest

		if err := c.Bind().Body(&body); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		failedSubmissions, err := service.AddSubmissions(c.Context(), body.Submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		c.Status(fiber.StatusCreated)
		return c.JSON(failedSubmissions)
	}
}

func Vote(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body voteRequest

		if err := c.Bind().Body(&body); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		successfulSubs, err := service.UpdateVotes(c.Context(), body.Submissions, body.Vote)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(successfulSubs)
	}
}

func AcceptSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body submissionPostRequest

		if err := c.Bind().Body(&body); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		successfulSubs, err := service.AcceptSubmissions(c.Context(), body.Submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(successfulSubs)
	}
}

func RejectSubmissions(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body submissionPostRequest

		if err := c.Bind().Body(&body); err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusBadRequest)
		}

		successfulSubmissions, err := service.RejectSubmissions(c.Context(), body.Submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return c.JSON(successfulSubmissions)
	}
}
