package servers

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

type CoordinatorServer struct {
	workersMutex sync.Mutex
	Workers      []string
	Files        []string
	MinWorkers   int
}

func (s *CoordinatorServer) Register(addr *string, reply *bool) error {
	s.workersMutex.Lock()
	s.Workers = append(s.Workers, *addr)
	log.Printf("[DEBUG] Registering %v worker\n", *addr)
	s.workersMutex.Unlock()
	return nil
}

type WorkerServer struct {
}

type KeyValue struct {
	Key   string
	Value string
}

type MapRPCRequest struct {
	Key   string
	Value string
}

type MapRPCReply struct {
	KVA []KeyValue
}

func (w *WorkerServer) Map(req *MapRPCRequest, resp *MapRPCReply) error {
	// function to detect word separators.
	log.Println("[DEBUG] map invoked.")
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc((*req).Value, ff)

	altKVA := []KeyValue{}

	for _, w := range words {
		kv := KeyValue{Key: w, Value: "1"}
		altKVA = append(altKVA, kv)
	}

	resp.KVA = altKVA
	return nil
}

type ReduceRPCRequest struct {
	Key    string
	Values []string
}

type ReduceRPCReply struct {
	Values []string
}

func (w *WorkerServer) Reduce(req *ReduceRPCRequest, resp *ReduceRPCReply) error {
	log.Println("[DEBUG] reduce invoked.")
	resp.Values = []string{strconv.Itoa(len((*req).Values))}
	return nil
}

func (w *WorkerServer) Shutdown(req *bool, resp *bool) error {
	log.Println("[DEBUG] shutdown invoked.")
	os.Exit(0)
	return nil
}
