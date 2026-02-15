package contentProcessing

import (
	"bytes"
	english "indexer/internals/snowball"
	"indexer/internals/storage/redis"
	"indexer/internals/utils"
	"strings"
	"unicode"

	snowball "github.com/snowballstem/snowball/go"
	"golang.org/x/net/html"
)

func ProcessCrawledPage(crawledPageContent *redis.CrawledPage) (*utils.IndexerPage, error) {
	doc, err := html.Parse(bytes.NewBufferString(crawledPageContent.HtmlContent))
	if err != nil {
		return nil, err
	}

	var textContent strings.Builder
	var textContentSummary strings.Builder

	processedPage := &utils.IndexerPage{
		Url:       crawledPageContent.Url,
		Timestamp: crawledPageContent.Timestamp,
		Outlinks:  &crawledPageContent.Outlinks,
	}

	for descendant := range doc.Descendants() {
		if descendant.Type == html.ElementNode {
			switch descendant.Data {
			case "title":
				processedPage.Title = string(descendant.FirstChild.Data)
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
							processedPage.Description = attr.Val
						}
					}
				}
			case "main":
				for mainDescendant := range descendant.Descendants() {
					switch mainDescendant.Data {
					case "h1", "h2", "h3", "h4", "h5", "h6", "h7", "blockquote", "p":
						for textDescendant := range mainDescendant.Descendants() {
							switch textDescendant.Parent.Data {
							case "annotation", "style", "script":
								continue
							}
							if textDescendant.Type == html.TextNode {
								textContent.WriteString(textDescendant.Data)
								if mainDescendant.Data == "p" {
									textContentSummary.WriteString(textDescendant.Data)
								}
							}
						}
					}
				}

			}
		}
	}

	processedPage.TextContentSummary = textContentSummary.String()

	words := strings.Fields(processedPage.TextContentSummary)
	if len(words) > 200 {
		processedPage.TextContentSummary = strings.Join(words[:200], " ")
	}

	cleanedTextContent := removeStopWords(strings.FieldsFunc(textContent.String(), func(r rune) bool {
		// Split on punctuation except apostrophes, or whitespace
		if unicode.IsSpace(r) {
			return true
		}
		if unicode.IsPunct(r) && r != '\'' {
			return true
		}
		return false
	}))

	// Disabling stemming for now, for testing more precise indexing and querying
	// stemmedTextContent := stemWords(cleanedTextContent)
	processedPage.CleanTextContent = &cleanedTextContent

	return processedPage, nil
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
