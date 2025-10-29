package indexer

import (
	"bytes"
	"indexer/internals/queue"
	english "indexer/internals/snowball"
	"strings"
	"unicode"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	snowball "github.com/snowballstem/snowball/go"
	"golang.org/x/net/html"
)

type processedJob struct {
	rawTextContent   string
	cleanTextContent []string
	title            string
	description      string
}

func (i *Indexer) processJob(job *queue.IndexJob) (*processedJob, error) {
	tokenizer := html.NewTokenizer(bytes.NewBufferString(job.HtmlContent))
	var textContent string
	result := processedJob{}

	for {
		tokenType := tokenizer.Next()

		switch tokenType {
		case html.ErrorToken:
			result.rawTextContent = textContent
			cleanedTextContent := i.removeStopWords(strings.FieldsFunc(textContent, func(r rune) bool {
				// Split on punctuation except apostrophes, or whitespace
				if unicode.IsSpace(r) {
					return true
				}
				if unicode.IsPunct(r) && r != '\'' {
					return true
				}
				return false
			}))
			stemmedTextContent := i.stemWords(cleanedTextContent)
			result.cleanTextContent = stemmedTextContent
			return &result, tokenizer.Err() // if err is io.eof then indicates End of the document
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			switch token.Data {
			case "script", "noscript", "style":
				tokenizer.Next() // Skip the content of non text tags
			case "title":
				tokenizer.Next()
				result.title = string(tokenizer.Text())
			case "meta":
				key := ""
				for _, attr := range token.Attr {
					switch attr.Key {
					case "name":
						if attr.Val == "description" {
							key = "d"
						}
					case "content":
						if key == "d" {
							result.description = attr.Val
						}
					}
				}
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
		if !isStopWord(word) {
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
