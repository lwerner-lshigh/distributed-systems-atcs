package servers

import (
	"log"
	"net/rpc"
	"sync"
)

type WorkerError struct {
	Worker        string
	SelectedSlice int
	Key           string
	Error         error
}

func PreformMap(wg *sync.WaitGroup, mu *sync.Mutex, resultsList *[][]KeyValue, worker string, selectedSlice int, key string, data string, errors chan WorkerError) {
	defer wg.Done()
	client, err := rpc.DialHTTP("tcp", worker)
	if err != nil {
		// TODO: re-organize the worker submit job to also fail & heal
		log.Println("[PreformMap] dialing:", err)
		errors <- WorkerError{worker, selectedSlice, key, err}
		return
	}
	defer client.Close()
	resp := &MapRPCReply{}
	err = client.Call("WorkerServer.Map", &MapRPCRequest{
		Key:   key,
		Value: data,
	}, resp)
	if err != nil {
		log.Printf("map [%v] exec: %v\n", worker, err)
		errors <- WorkerError{worker, selectedSlice, key, err}
		return
	}
	log.Printf("[DEBUG] %v map finished execution\n", worker)
	mu.Lock()
	slice := (*resultsList)
	(*resultsList) = append(slice, resp.KVA)
	mu.Unlock()
	log.Printf("[DEBUG] pushing %v results on to the chan\n", worker)
}

func PreformReduce(wg *sync.WaitGroup, mu *sync.Mutex, guard chan bool, key string, values []string, reduceResults map[string]string, worker string, selectedIndex int, errors chan WorkerError) {
	defer wg.Done()
	client, err := rpc.DialHTTP("tcp", worker)
	if err != nil {
		<-guard
		log.Printf("[PreformReduce] dialing: was unable to dial worker: %s", err)
		errors <- WorkerError{worker, selectedIndex, key, err}

		return
	}
	defer client.Close()
	resp := &ReduceRPCReply{}
	err = client.Call("WorkerServer.Reduce", &ReduceRPCRequest{
		Key:    key,
		Values: values,
	}, resp)
	if err != nil {
		<-guard
		log.Printf("[PreformReduce] guard: %#v", len(guard))
		log.Printf("[PreformReduce] (RPC).reduce: [%v] exec: %v\n", worker, err)
		errors <- WorkerError{worker, selectedIndex, key, err}

		return
	}

	mu.Lock()
	reduceResults[key] = resp.Values[0]
	mu.Unlock()
	<-guard
}
