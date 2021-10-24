package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

func main() {
	if len(os.Args) <= 1 {
		panic("Not enough arguments passed please specify input file")
	}

	// read the input file
	f, err := os.Open(os.Args[1])
	defer f.Close()
	if err != nil {
		log.Fatalf("Was unable to open file: %s", err)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("Was unable to read file: %s", err)
	}

	// chunking of data
	parts := 3
	data := string(b)

	chunks := []string{}
	chunkSize := (len(b) + parts - 1) / parts
	fmt.Println(chunkSize)
	for i := 0; i < len(b)-1; i += chunkSize {
		chunks = append(chunks, data[i:i+chunkSize])
	}

	var wg sync.WaitGroup
	var writeMutex sync.Mutex
	var intermediateData [][]KeyValue

	// do the maping of values, returns the keyvalue slice
	for _, chunk := range chunks {
		wg.Add(1)
		go func(achunk string) {
			defer wg.Done()
			result := Map(os.Args[1], achunk)
			writeMutex.Lock()
			intermediateData = append(intermediateData, result)
			writeMutex.Unlock()
		}(chunk)
	}
	wg.Wait()

	fmt.Println(intermediateData)

	// do a data shuffle

	// do a reduce of the keys and values so we get useful results

	// close all jobs
}

type KeyValue struct {
	Key   string
	Value string
}

func Map(key string, value string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(value, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}

	time.Sleep(2 * time.Second)
	return kva
}

func Reduce(key string, values []string) []string {
	return []string{strconv.Itoa(len(values))}
}
