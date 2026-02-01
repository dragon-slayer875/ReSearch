package indexer

import (
	"bytes"
	english "indexer/internals/snowball"
	"indexer/internals/storage/postgres"
	"indexer/internals/storage/redis"
	"strings"
	"unicode"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	snowball "github.com/snowballstem/snowball/go"
	"golang.org/x/net/html"
)

func (w *Worker) processWebpage(job *redis.IndexJob) (*postgres.ProcessedJob, error) {
	doc, err := html.Parse(bytes.NewBufferString(job.HtmlContent))
	if err != nil {
		return nil, err
	}

	var textContent string
	result := postgres.ProcessedJob{}

	for descendant := range doc.Descendants() {
		if descendant.Type == html.ElementNode {
			switch descendant.Data {
			case "title":
				result.Title = string(descendant.FirstChild.Data)
			case "meta":
				key := false
			attr_loop:
				for _, attr := range descendant.Attr {
					switch attr.Key {
					case "name":
						if attr.Val == "description" {
							key = true
						} else {
							break attr_loop
						}
					case "content":
						if key {
							result.Description = attr.Val
						}
					}
				}
			case "main":
				for mainDescendant := range descendant.Descendants() {
					switch mainDescendant.Data {
					case "p", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "blockquote":
						for textDescendant := range mainDescendant.Descendants() {
							if textDescendant.Parent.Data == "annotation" {
								continue
							}
							if textDescendant.Type == html.TextNode {
								textContent += " " + textDescendant.Data
							}
						}
					}
				}

			}
		}
	}

	result.RawTextContent = textContent
	if len(textContent) > 100 {
		result.RawTextContent = textContent[:100]
	}

	cleanedTextContent := w.removeStopWords(strings.FieldsFunc(textContent, func(r rune) bool {
		// Split on punctuation except apostrophes, or whitespace
		if unicode.IsSpace(r) {
			return true
		}
		if unicode.IsPunct(r) && r != '\'' {
			return true
		}
		return false
	}))
	stemmedTextContent := w.stemWords(cleanedTextContent)
	result.CleanTextContent = stemmedTextContent

	return &result, nil

}

func (w *Worker) removeStopWords(content []string) []string {
	var filteredWords []string

	for _, word := range content {
		word = strings.ToLower(word)
		if !isStopWord(word) {
			filteredWords = append(filteredWords, word)
		}
	}

	return filteredWords
}

func (w *Worker) stemWords(content []string) []string {
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

func (w *Worker) createPostingsList(cleanedTextContent []string, docId int64) (*map[string]*Posting, error) {
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
