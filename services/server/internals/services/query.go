package services

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"server/internals/storage/database"
	"server/internals/storage/redis"
	"slices"
	"strings"

	"github.com/hbollon/go-edlib"
	"github.com/jackc/pgx/v5/pgxpool"
	redisLib "github.com/redis/go-redis/v9"
)

type searchResultsCacheData struct {
	Results    []database.GetSearchResultsRow
	TotalPages int64
}

type SearchService struct {
	queries *database.Queries
	rClient *redisLib.Client
}

func NewSearchService(pool *pgxpool.Pool, rClient *redisLib.Client) *SearchService {
	return &SearchService{
		database.New(pool),
		rClient,
	}
}

func (l *SearchService) GetSearchResults(ctx context.Context, queryWords *[]string, limit, page int32, useCache bool) (*[]database.GetSearchResultsRow, int64, error) {

	if useCache {
		queryKey := redis.SearchResultsCachePrefix + strings.Join(*queryWords, "")
		cacheGetCmd := l.rClient.Get(ctx, queryKey)
		if err := cacheGetCmd.Err(); err != nil && err != redisLib.Nil {
			return nil, 0, err
		} else if err == nil {
			cacheData := new(searchResultsCacheData)

			err = json.Unmarshal([]byte(cacheGetCmd.Val()), cacheData)
			if err != nil {
				return nil, 0, err
			}

			return &cacheData.Results, cacheData.TotalPages, nil
		}
	}

	count, err := l.queries.GetSearchResultCount(ctx, *queryWords)
	if err != nil {
		return nil, 0, err
	}

	results, err := l.queries.GetSearchResults(ctx, database.GetSearchResultsParams{
		Column1: *queryWords,
		Limit:   limit,
		Offset:  (page - 1) * limit,
	})
	if err != nil {
		return nil, 0, err
	}

	return &results, int64(math.Ceil(float64(count) / float64(limit))), nil
}

func (ss *SearchService) GetSuggestions(ctx context.Context, query string) (*[]string, string, *[]string, error) {
	var suggestion string
	querySplit := strings.Fields(query)
	suggestedWords := make([]string, len(querySplit))

	// Disabling stop word detection since people can search for anything

	// contentWordIndices := make([]int, 0, len(querySplit))
	// contentWords := make([]string, 0, len(querySplit))
	//
	// for idx, word := range querySplit {
	// 	if !utils.IsStopWord(word) {
	// 		contentWordIndices = append(contentWordIndices, idx)
	// 		contentWords = append(contentWords, word)
	// 	}
	// }

	wordsAndSuggestions := make([]string, len(querySplit)*2)

	suggestions, err := ss.rClient.HMGet(ctx, redis.DictionaryKey, querySplit...).Result()
	if err != nil {
		return nil, "", nil, err
	}

	cacheSuggestionsMiss := slices.Contains(suggestions, nil)

	if !cacheSuggestionsMiss {
		for idx, word := range querySplit {
			suggestedWords[idx] = (suggestions[idx]).(string)
			wordsAndSuggestions[idx*2] = word
			wordsAndSuggestions[idx*2+1] = (suggestions[idx]).(string)
		}

		suggestion = strings.Join(suggestedWords, " ")

		return &querySplit, suggestion, &wordsAndSuggestions, nil
	}

	indexedWords, err := ss.queries.GetIndexedWords(ctx)
	if err != nil {
		return nil, "", nil, err
	}

	for idx, word := range querySplit {
		wordsAndSuggestions[idx*2] = word
		if suggestions[idx] == nil {
			// The err is left unchecked because the FuzzySearch only returns an error in two cases,
			// either while using hamming distance or an invalid algorithm identifier
			res, _ := edlib.FuzzySearch(word, indexedWords, edlib.Levenshtein)
			if res == "" {
				return &querySplit, suggestion, &[]string{}, nil
			}
			suggestedWords[idx] = res
			wordsAndSuggestions[idx*2+1] = res
		} else {
			wordsAndSuggestions[idx*2+1] = (suggestions[idx]).(string)
		}
	}

	suggestion = strings.Join(suggestedWords, " ")

	return &querySplit, suggestion, &wordsAndSuggestions, nil
}

func (ss *SearchService) CacheQueryData(ctx context.Context, queryContentWords *[]string, queryResults *[]database.GetSearchResultsRow, totalPages int64, wordsAndSugesstions *[]string, isFirstPage bool) error {
	// Caching for one hour for now
	pipe := ss.rClient.TxPipeline()

	if isFirstPage && len(*queryResults) > 0 {
		queryData := searchResultsCacheData{
			Results:    *queryResults,
			TotalPages: totalPages,
		}

		if len(*queryResults) > 10 {
			queryData.Results = (*queryResults)[:10]
		}

		data, err := json.Marshal(queryData)
		if err != nil {
			return err
		}

		queryKey := redis.SearchResultsCachePrefix + strings.Join(*queryContentWords, "")
		pipe.Set(ctx, queryKey, data, time.Hour)
	}

	if len(*wordsAndSugesstions) > 0 {
		pipe.HSetEXWithArgs(ctx, redis.DictionaryKey, &redisLib.HSetEXOptions{
			ExpirationType: redisLib.HSetEXExpirationEX,
			ExpirationVal:  3600,
		}, *wordsAndSugesstions...)
	}

	_, err := pipe.Exec(ctx)
	return err
}
