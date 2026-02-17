package handlers

import (
	"server/internals/services"
	"server/internals/templates"
	"server/internals/utils"
	"strconv"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

func ServeIndex() fiber.Handler {
	return func(c fiber.Ctx) error {
		return utils.Render(c, templates.IndexPage())
	}
}

func ServeResults(service *services.SearchService) fiber.Handler {
	return func(c fiber.Ctx) error {
		req := new(utils.SearchGetRequest)

		if err := c.Bind().Query(req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		isFirstPage := req.Page == 1

		contentWords, suggestion, wordsAndSuggestions, err := service.GetSuggestions(c.Context(), req.Query)
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

		if suggestion == req.Query {
			suggestion = ""
		}

		return utils.Render(c, templates.ResultsPage(&utils.ResultsPageData{
			SearchResults: query_results,
			TotalPages:    totalPages,
			CurrentPage:   int64(req.Page),
			Suggestion:    suggestion,
			Query:         req.Query,
		}))
	}
}

func GetCrawlerboardPage(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req submissionGetRequest

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		submissions, err := service.GetSubmissions(c.Context(), req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return err
		}

		return utils.Render(c, templates.Crawlerboard(submissions))
	}
}

func AddUrlToCrawlerboard(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req submissionGetRequest

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		submission := c.FormValue("submission")

		_, err := service.AddSubmissions(c.Context(), []string{submission})
		if err != nil {
			log.Error(err)
			return err
		}

		c.Status(fiber.StatusCreated)

		submissions, err := service.GetSubmissions(c.Context(), req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return err
		}

		return utils.Render(c, templates.CrawlerboardContent(submissions))
	}

}

func RejectCrawlerboardPage(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req map[string]string

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		submissionsToReject := make([]string, 0, len(req))

		for submission := range req {
			submissionsToReject = append(submissionsToReject, submission)
		}

		rejectedSubmissions, err := service.RejectSubmissions(c.Context(), submissionsToReject)
		if err != nil {
			log.Error(err)
			return err
		}

		return c.SendString(strconv.Itoa(int(rejectedSubmissions)))
		// submissions, err := service.GetSubmissions(c.Context(), req.Order, req.Page, req.Limit)
		// if err != nil {
		// 	log.Error(err)
		// 	return err
		// }
		//
		// return utils.Render(c, templates.CrawlerboardContent(submissions))
	}
}
