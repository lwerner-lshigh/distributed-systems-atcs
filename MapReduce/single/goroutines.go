package main

import (
	"encoding/json"
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

	start := time.Now()

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

	took := time.Since(start)

	log.Print("[DEBUG] Finished reading; Took ", took)

	// chunking of data

	start = time.Now()

	parts := 4
	data := string(b)

	chunks := []string{}
	chunkSize := (len(b) + parts - 1) / parts
	offset := 0
	//fmt.Println(chunkSize)
	for i := 0; i < len(b)-1; i += chunkSize {
		if i+chunkSize+offset > len(b) {
			log.Printf("[i=%v] size=%v chunkSize=%v parts=%v SPECIAL CASE! slice[%v:%v]\n", i, len(b), chunkSize, parts, i, i+len(b)-1)
			chunks = append(chunks, data[i+offset:len(b)-1-offset])
		} else {
			if unicode.IsSpace(rune(data[i+offset+chunkSize])) {
				log.Printf("[i=%v] size=%v chunkSize=%v parts=%v\n", i, len(b), chunkSize, parts)
				chunks = append(chunks, data[i+offset:i+chunkSize+offset])
			} else {
				for !unicode.IsSpace(rune(data[i+offset])) { // so that we dont cut off in the middle of a word (in text data)
					offset++
					//log.Printf("Found case of not space: %c\n", data[i+offset])
				}
				log.Printf("[i=%v] size=%v chunkSize=%v parts=%v\n", i, len(b), chunkSize, parts)
				chunks = append(chunks, data[i+offset:i+chunkSize+offset])
			}

		}
	}

	took = time.Since(start)

	log.Print("[DEBUG] Finished chunking; Took ", took)

	start = time.Now()
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

	took = time.Since(start)

	log.Print("[DEBUG] Finished Map; Took ", took)

	//fmt.Println(intermediateData)

	// do a data shuffle

	start = time.Now()

	shuffle := make(map[string][]string)

	for _, result := range intermediateData {
		for _, kv := range result {
			shuffle[kv.Key] = append(shuffle[kv.Key], kv.Value)
		}
	}

	took = time.Since(start)

	log.Print("[DEBUG] Finished shuffle; Took ", took)

	// do a reduce of the keys and values so we get useful results

	start = time.Now()

	results := make(map[string]string)

	for key, value := range shuffle {
		wg.Add(1)
		go func(key string, value []string) {
			defer wg.Done()
			result := Reduce(key, value)
			writeMutex.Lock()
			results[key] = result[0]
			writeMutex.Unlock()
		}(key, value)
	}

	// close all jobs

	wg.Wait()

	took = time.Since(start)

	log.Print("[DEBUG] Finished reduce; Took ", took)

	start = time.Now()

	b, err = json.MarshalIndent(results, "", "\t")
	if err != nil {
		log.Fatalf("Encountered error during marshaling: %v", err)
	}

	f, err = os.Create("mapreduce-results.out")
	if err != nil {
		log.Fatalf("Encountered error during file creation: %v", err)
	}
	defer f.Close()

	_, err = f.Write(b)
	if err != nil {
		log.Fatalf("Encountered error during file writing: %v", err)
	}
	took = time.Since(start)

	log.Print("[DEBUG] Finished writing; Took ", took)

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
