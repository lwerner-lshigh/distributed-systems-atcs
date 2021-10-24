package main

import (
	"strconv"
	"strings"
	"unicode"
)

// Coppied from 6.284 Distributed Systems class from MIT
func Map(filename, contents string) []map[string]string {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []map[string]string{}
	for _, w := range words {
		kv := map[string]string{w: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
