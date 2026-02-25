package handlers

import (
	"fmt"
	"math"
	"server/internals/services"
	"server/internals/templates"
	"server/internals/utils"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
)

type searchRequest struct {
	// Sort  string `query:"sort,default:score" validate:"oneof=score time"`
	// Order string `query:"order,default:dsc" validate:"oneof=asc dsc"`
	Limit int32  `query:"limit,default:10" validate:"gt=0"`
	Page  int32  `query:"page,default:1" validate:"gt=0"`
	Query string `query:"query"`
}

type addRequest struct {
	Submissions string `json:"submissions" form:"submissions" validate:"required"`
}

type acceptRejectRequest struct {
	Submissions []string `json:"submissions" form:"submissions" query:"submissions" validate:"required"`
}

type paginationParams struct {
	Order string `query:"order,default:new" validate:"oneof=old new"`
	Page  int    `query:"page,default:1" validate:"gt=0"`
	Limit int    `query:"limit,default:10" validate:"gt=0"`
}

func ServeIndex() fiber.Handler {
	return func(c fiber.Ctx) error {
		if c.Get("accept") == "application/json" {
			return c.SendString("Welcome to reSearch")
		}

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

		if !admin {
			return c.Status(fiber.StatusUnauthorized).SendString("Missing or invalid admin key")
		}

		if c.Get("accept") == "application/json" {
			return c.SendString("admin")
		}

		c.Cookie(&fiber.Cookie{
			Name:     "rs_key",
			Value:    c.FormValue("rs_key"),
			SameSite: "Lax",
			HTTPOnly: true,
			Secure:   true,
		})

		return c.Redirect().To("/crawlerboard")

	}
}

func ServeResults(service *services.SearchService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req searchRequest

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		isFirstPage := req.Page == 1
		useCache := req.Page == 1 && req.Limit == 10

		contentWords, suggestion, wordsAndSuggestions, err := service.GetSuggestions(c.Context(), req.Query)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		query_results, totalPages, err := service.GetSearchResults(c.Context(), contentWords, req.Limit, req.Page, useCache)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		ctx := c.Context()
		go func() {
			err = service.CacheQueryData(ctx, contentWords, query_results, totalPages, wordsAndSuggestions, isFirstPage)
			if err != nil {
				log.Error(err)
			}
		}()

		if suggestion == req.Query {
			suggestion = ""
		}

		data := &utils.ResultsPageData{
			SearchResults: query_results,
			TotalPages:    totalPages,
			CurrentPage:   int64(req.Page),
			Suggestion:    suggestion,
			Query:         req.Query,
		}

		if c.Get("accept") == "application/json" {
			return c.JSON(*data)
		}

		return utils.Render(c, templates.ResultsPage(data))
	}
}

func CrawlerboardGet(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req paginationParams

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest)
		}

		submissions, total, err := service.GetSubmissions(c.Context(), req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		totalPages := int64(math.Ceil(float64(total) / float64(req.Limit)))

		if c.Get("accept") == "application/json" {
			return c.JSON(map[string]any{
				"Submissions": *submissions,
				"TotalPages":  totalPages,
			})
		}

		if int64(req.Page) > totalPages {
			submissions, total, err = service.GetSubmissions(c.Context(), req.Order, 1, req.Limit)
			if err != nil {
				log.Error(err)
				return fiber.NewError(fiber.StatusInternalServerError)
			}
		}

		totalPages = int64(math.Ceil(float64(total) / float64(req.Limit)))

		data := &utils.CrawlerboardPageData{
			Submissions: submissions,
			TotalPages:  totalPages,
			CurrentPage: int64(req.Page),
			Limit:       req.Limit,
			Order:       req.Order,
			Admin:       (c.Value("auth")).(bool),
		}

		if c.Get("HX-Request") == "true" {
			return utils.Render(c, templates.CrawlerboardTable(data))
		}

		return utils.Render(c, templates.Crawlerboard(data))
	}
}

func CrawlerboardAdd(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body addRequest

		if err := c.Bind().Body(&body); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		submissions := strings.Split(body.Submissions, ",")

		successful, failed, err := service.AddSubmissions(c.Context(), &submissions)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		if len(*successful) != 0 && len(*failed) != 0 {
			c.Status(fiber.StatusMultiStatus)
			c.Set("HX-Trigger", "crawlerboard-updated")
		} else if len(*failed) > 1 {
			c.Status(fiber.StatusMultiStatus)
		} else if _, ok := (*failed)[services.ConflictingUrls]; ok {
			c.Status(fiber.StatusConflict)
		} else if _, ok := (*failed)[services.MalformedUrls]; ok {
			c.Status(fiber.StatusUnprocessableEntity)
		} else if len(*successful) != 0 {
			c.Status(fiber.StatusCreated)
			c.Set("HX-Trigger", "crawlerboard-updated")
		}

		if c.Get("accept") == "application/json" {
			return c.JSON(map[string]any{
				"Successful": *successful,
				"Failed":     *failed,
			})
		}

		notifications := new(utils.Notifications)
		notifications.Failure = map[string]string{}

		if len(*successful) != 0 {
			notifications.Success = fmt.Sprintln(len(*successful), "url(s) added")
		}

		for error, output := range *failed {
			notifications.Failure[error] = strings.Join(output, ", ")
		}

		return utils.Render(c, templates.Notify(notifications))
	}

}

func CrawlerboardReject(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req paginationParams
		var data acceptRejectRequest

		if err := c.Bind().All(&data); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		successful, failed, err := service.RejectSubmissions(c.Context(), &data.Submissions, req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		c.Set("HX-Trigger", "crawlerboard-updated")

		if len(*successful) != 0 && len(*failed) != 0 {
			c.Status(fiber.StatusMultiStatus)
		} else if _, ok := (*failed)[services.NotFoundUrls]; ok {
			c.Status(fiber.StatusNotFound)
			c.Set("HX-Trigger", "")
		}

		if c.Get("accept") == "application/json" {
			return c.JSON(map[string]any{
				"Successful": *successful,
				"Failed":     *failed,
			})
		}

		notifications := new(utils.Notifications)
		notifications.Failure = map[string]string{}

		if len(*successful) != 0 {
			notifications.Success = fmt.Sprintln(len(*successful), "urls rejected")
		}

		for error, output := range *failed {
			notifications.Failure[error] = strings.Join(output, ", ")
		}

		return utils.Render(c, templates.Notify(notifications))
	}
}

func CrawlerboardAccept(service *services.CrawlerBoardService) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req paginationParams
		var data acceptRejectRequest

		if err := c.Bind().All(&data); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		if err := c.Bind().Query(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		successful, failed, err := service.AcceptSubmissions(c.Context(), &data.Submissions, req.Order, req.Page, req.Limit)
		if err != nil {
			log.Error(err)
			return fiber.NewError(fiber.StatusInternalServerError)
		}

		c.Set("HX-Trigger", "crawlerboard-updated")

		if len(*successful) != 0 && len(*failed) != 0 {
			c.Status(fiber.StatusMultiStatus)
		} else if _, ok := (*failed)[services.NotFoundUrls]; ok {
			c.Status(fiber.StatusNotFound)
			c.Set("HX-Trigger", "")
		}

		if c.Get("accept") == "application/json" {
			return c.JSON(map[string]any{
				"Successful": *successful,
				"Failed":     *failed,
			})
		}

		notifications := new(utils.Notifications)
		notifications.Failure = map[string]string{}

		if len(*successful) != 0 {
			notifications.Success = fmt.Sprintln(len(*successful), "urls accepted")
		}

		for error, output := range *failed {
			notifications.Failure[error] = strings.Join(output, ", ")
		}

		return utils.Render(c, templates.Notify(notifications))
	}
}
