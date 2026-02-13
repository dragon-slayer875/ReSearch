package utils

import "server/internals/storage/database"

type SearchGetRequest struct {
	// Sort  string `query:"sort,default:score" validate:"oneof=score time"`
	// Order string `query:"order,default:dsc" validate:"oneof=asc dsc"`
	Limit int32  `query:"limit,default:10" validate:"gt=0"`
	Page  int32  `query:"page,default:1" validate:"gt=0"`
	Query string `query:"query"`
}

type SearchGetResponse struct {
	Pages   int                            `json:"pages"`
	Count   int64                          `json:"count"`
	Results []database.GetSearchResultsRow `json:"results"`
}
