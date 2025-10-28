package utils

import (
	snowball "github.com/snowballstem/snowball/go"
	english "query_engine/internals/snowball"
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
