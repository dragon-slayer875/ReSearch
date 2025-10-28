package utils

import (
	english "query_engine/internals/snowball"
	"strings"

	snowball "github.com/snowballstem/snowball/go"
)

func StemWords(content []string) []string {
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

func RemoveStopWords(content []string) []string {
	var filteredWords []string

	for _, word := range content {
		word = strings.ToLower(word)
		if !isStopWord(word) {
			filteredWords = append(filteredWords, word)
		}
	}

	return filteredWords
}
