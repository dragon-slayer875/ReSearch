package utils

import (
	"strings"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type Posting struct {
	DocId     int64             `json:"doc_id"`    // Document ID
	Tf        int32             `json:"tf"`        // Term frequency
	Positions *roaring64.Bitmap `json:"positions"` // Positions of the term in the document
}

type InvertedIndex struct {
	Word      string            `json:"word"`       // The term
	DocBitmap *roaring64.Bitmap `json:"doc_bitmap"` // Bitmap of document IDs containing the term
	DocFreq   int64             `json:"doc_freq"`   // Document frequency
}

func CreatePostingsList(cleanedTextContent []string, docId int64) (*map[string]*Posting, error) {
	postingsList := make(map[string]*Posting)

	for pos, word := range cleanedTextContent {
		word = strings.ToLower(word)
		if _, exists := postingsList[word]; !exists {
			postingsList[word] = &Posting{
				DocId:     docId,
				Tf:        1,
				Positions: roaring64.New(),
			}

			postingsList[word].Positions.Add(uint64(pos))

		} else {
			postingsList[word].Tf++
			postingsList[word].Positions.Add(uint64(pos))
		}
	}

	return &postingsList, nil
}
