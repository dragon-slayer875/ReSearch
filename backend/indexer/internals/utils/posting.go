package utils

import (
	"strings"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type Posting struct {
	DocUrl    string            `json:"doc_url"`   // Document ID
	Tf        int32             `json:"tf"`        // Term frequency
	Positions *roaring64.Bitmap `json:"positions"` // Positions of the term in the document
}

type InvertedIndex struct {
	Word    string `json:"word"`     // The term
	DocFreq int64  `json:"doc_freq"` // Document frequency
	// Will be using SQL queries for fetching query results for now so no need for document bitmaps
	// DocBitmap *roaring64.Bitmap `json:"doc_bitmap"` // Bitmap of document IDs containing the term
}

func CreatePostingsList(cleanedTextContent *[]string, docUrl string) *map[string]*Posting {
	postingsList := make(map[string]*Posting)

	for pos, word := range *cleanedTextContent {
		word = strings.ToLower(word)
		if _, exists := postingsList[word]; !exists {
			postingsList[word] = &Posting{
				DocUrl:    docUrl,
				Tf:        1,
				Positions: roaring64.New(),
			}

			postingsList[word].Positions.Add(uint64(pos))

		} else {
			postingsList[word].Tf++
			postingsList[word].Positions.Add(uint64(pos))
		}
	}

	return &postingsList
}
