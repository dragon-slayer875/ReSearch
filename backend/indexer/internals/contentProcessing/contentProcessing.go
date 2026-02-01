package contentProcessing

import (
	english "indexer/internals/snowball"
	"strings"
	"unicode"

	"bytes"
	snowball "github.com/snowballstem/snowball/go"
	"golang.org/x/net/html"
	"indexer/internals/storage/postgres"
	"indexer/internals/storage/redis"
)

func ProcessWebpage(job *redis.IndexJob) (*postgres.ProcessedJob, error) {
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

	cleanedTextContent := removeStopWords(strings.FieldsFunc(textContent, func(r rune) bool {
		// Split on punctuation except apostrophes, or whitespace
		if unicode.IsSpace(r) {
			return true
		}
		if unicode.IsPunct(r) && r != '\'' {
			return true
		}
		return false
	}))
	stemmedTextContent := stemWords(cleanedTextContent)
	result.CleanTextContent = stemmedTextContent

	return &result, nil
}

func removeStopWords(content []string) []string {
	var filteredWords []string

	for _, word := range content {
		word = strings.ToLower(word)
		if !isStopWord(word) {
			filteredWords = append(filteredWords, word)
		}
	}

	return filteredWords
}

func stemWords(content []string) []string {
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
