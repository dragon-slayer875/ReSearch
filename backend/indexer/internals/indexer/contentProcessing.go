package indexer

import (
	"bytes"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	snowball "github.com/snowballstem/snowball/go"
	"golang.org/x/net/html"
	"indexer/internals/queue"
	english "indexer/internals/snowball"
	"strings"
)

func (i *Indexer) getCleanTextContent(job *queue.IndexJob) ([]string, error) {
	tokenizer := html.NewTokenizer(bytes.NewBufferString(job.HtmlContent))
	var textContent string

	for {
		tokenType := tokenizer.Next()

		switch tokenType {
		case html.ErrorToken:
			cleanedTextContent := i.removeStopWords(strings.Fields(textContent))
			stemmedTextContent := i.stemWords(cleanedTextContent)
			return stemmedTextContent, nil // End of the document
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			switch token.Data {
			case "script", "noscript", "style":
				tokenizer.Next() // Skip the content of non text tags
			}
		case html.TextToken:
			token := tokenizer.Token()
			textContent += " " + token.Data
		}
	}

}

func (i *Indexer) removeStopWords(content []string) []string {
	var filteredWords []string

	for _, word := range content {
		word = strings.ToLower(word)
		if _, exists := stopWords[word]; !exists {
			filteredWords = append(filteredWords, word)
		}
	}

	return filteredWords
}

func (i *Indexer) stemWords(content []string) []string {
	var stemmedWords []string
	env := snowball.NewEnv("")
	english.Stem(env)

	for _, word := range content {
		env.SetCurrent(word)
		english.Stem(env)
		stemmedWords = append(stemmedWords, env.Current())
	}

	return stemmedWords
}

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

func (i *Indexer) createPostingsList(cleanedTextContent []string, docId int64) (*map[string]*Posting, error) {
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
