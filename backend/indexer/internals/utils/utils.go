package utils

type IndexerPage struct {
	Url                string
	TextContentSummary string
	CleanTextContent   *[]string
	Title              string
	Description        string
	Outlinks           *[]string
	PostingsList       *map[string]*Posting
	Timestamp          int64
}
