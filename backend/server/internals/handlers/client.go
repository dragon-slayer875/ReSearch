package handlers

import (
	"fmt"
	"server/internals/services"
	"server/internals/templates"
	"server/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

func ServeIndex() fiber.Handler {
	return func(c fiber.Ctx) error {
		return utils.Render(c, templates.IndexPage())
	}
}

func ServeAdmin() fiber.Handler {
	return func(c fiber.Ctx) error {
		return utils.Render(c, templates.AdminPage())
	}
}

func VerifyAdmin() fiber.Handler {
	return func(c fiber.Ctx) error {
		admin := (c.Value("auth")).(bool)

		if admin {
			c.Cookie(&fiber.Cookie{
				Name:     "rs_key",
				Value:    c.FormValue("rs_key"),
				SameSite: "Lax",
				HTTPOnly: true,
				Secure:   true,
			})

			return c.Redirect().To("/crawlerboard")
		}

		c.Status(fiber.StatusUnauthorized)
		return c.SendString("Missing or invalid admin key")
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
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return utils.Render(c, templates.Crawlerboard(submissions, (c.Value("auth")).(bool)))
	}
}

func AddUrlToCrawlerboard(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		// var req submissionGetRequest
		//
		// if err := c.Bind().Query(&req); err != nil {
		// 	return fiber.NewError(fiber.StatusBadRequest)
		// }
		//
		submissions := strings.Split(c.FormValue("submission"), ",")

		successful, failed, err := service.AddSubmissions(c.Context(), &submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		notifications := new(utils.Notifications)
		notifications.Failure = map[string]string{}

		if len(*successful) != 0 {
			notifications.Success = "Urls added: " + strings.Join(*successful, ", ")
			c.Status(fiber.StatusCreated)
		} else if len(*successful) != 0 && len(*failed) != 0 {
			c.Status(fiber.StatusMultiStatus)
		} else {
			// c.Status(fiber.StatusBadRequest)
		}

		for error, output := range *failed {
			notifications.Failure[error] = strings.Join(output, ", ")
		}

		for _, submission := range *successful {
			err := utils.Render(c, templates.CrawlerboardEntry(submission, (c.Value("auth")).(bool)))
			if err != nil {
				log.Error(err)
				return fiber.NewError(fiber.StatusInternalServerError)
			}
		}

		return utils.Render(c, templates.Notify(notifications))

	}

}

func RejectCrawlerboardPage(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var formData map[string][]string
		var req submissionGetRequest

		if err := c.Bind().Query(&formData); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		submissionsToReject := formData["selected-urls"]

		submissions, successful, failed, err := service.RejectAndGetSubmissions(c.Context(), &submissionsToReject, req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		notifications := new(utils.Notifications)
		notifications.Failure = map[string]string{}

		if len(*successful) != 0 {
			notifications.Success = fmt.Sprintln(len(*successful), "urls rejected")
		} else if len(*successful) != 0 && len(*failed) != 0 {
			c.Status(fiber.StatusMultiStatus)
		} else {
			// c.Status(fiber.StatusBadRequest)
		}

		for error, output := range *failed {
			notifications.Failure[error] = strings.Join(output, ", ")
		}

		err = utils.Render(c, templates.CrawlerboardEntries(submissions, (c.Value("auth")).(bool)))
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return utils.Render(c, templates.Notify(notifications))
	}
}

func AcceptCrawlerboardPage(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var formData map[string][]string
		var req submissionGetRequest

		if err := c.Bind().Query(&formData); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		submissionsToAccept := formData["selected-urls"]

		submissions, successful, failed, err := service.AcceptSubmissions(c.Context(), &submissionsToAccept, req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		notifications := new(utils.Notifications)
		notifications.Failure = map[string]string{}

		if successful != 0 {
			notifications.Success = fmt.Sprintln(successful, "urls rejected")
		} else if successful != 0 && len(*failed) != 0 {
			c.Status(fiber.StatusMultiStatus)
		} else {
			// c.Status(fiber.StatusBadRequest)
		}

		for error, output := range *failed {
			notifications.Failure[error] = strings.Join(output, ", ")
		}

		err = utils.Render(c, templates.CrawlerboardEntries(submissions, (c.Value("auth")).(bool)))
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		return utils.Render(c, templates.Notify(notifications))
	}
}
