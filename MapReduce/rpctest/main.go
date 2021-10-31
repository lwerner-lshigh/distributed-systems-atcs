package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
	"unicode"

	"deltabeta.dev/rpctest/servers"
)

func main() {

	if len(os.Args) < 3 {
		log.Fatal("didnt have the right amount of args")
	}

	prog := os.Args[1]
	if prog == "client" {

		clientServer := new(servers.WorkerServer)
		rpc.Register(clientServer)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			log.Fatal("listen error:", err)
		}
		clientServer.ListenAddr = l.Addr().String()
		clientServer.CoordinatorServer = os.Args[2]

		go clientServer.HealthCheckRoutine()

		log.Println("Listening on", l.Addr().String())
		http.Serve(l, nil)

	}
	if prog == "server" {
		if len(os.Args) < 4 {
			log.Fatal("didnt have the right amount of args \n server/client\tminWorkers\tfiles...")
		}
		server := new(servers.CoordinatorServer)
		server.Files = append(server.Files, os.Args[3:]...)
		workers, err := strconv.Atoi(os.Args[2])
		server.MinWorkers = workers
		if err != nil {
			log.Fatal("minWorkers:", err)
		}
		rpc.Register(server)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", "0.0.0.0:1234")
		if err != nil {
			log.Fatal("listen error:", err)
		}
		log.Println("listening on port 1234")

		go server.HealthCheckRoutine()

		go func(serv *servers.CoordinatorServer) {
			completed := false
			for {
				// background proccess start loop
				time.Sleep(time.Millisecond * 100)
				// wait until we have the minimum workers available
				if serv.MinWorkers <= len(serv.Workers) {

					// we have enough workers

					for fileIndex, file := range serv.Files {

						start := time.Now()

						// read the input file
						f, err := os.Open(file)
						if err != nil {
							log.Fatalf("Was unable to open file: %s", err)
						}

						b, err := io.ReadAll(f)
						f.Close()
						if err != nil {
							log.Fatalf("Was unable to read file: %s", err)
						}

						took := time.Since(start)

						log.Print("[DEBUG] Finished reading; Took ", took)

						parts := len(serv.Workers)
						data := string(b)

						chunks := []string{}
						chunkSize := (len(b) + parts - 1) / parts
						offset := 0
						//fmt.Println(chunkSize)
						for i := 0; i < len(b)-1; i += chunkSize {
							if i+chunkSize+offset > len(b) {
								log.Printf("[i=%v] size=%v chunkSize=%v parts=%v SPECIAL END CASE! slice[%v:%v]\n", i, len(b), chunkSize, parts, i, i+len(b)-1)
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

						resultsList := [][]servers.KeyValue{}
						var mu sync.Mutex
						var wg sync.WaitGroup

						//results := make(chan []servers.KeyValue, len(serv.Workers))
						for i, worker := range serv.Workers {
							wg.Add(1)
							go func(worker string, data string) {
								defer wg.Done()
								client, err := rpc.DialHTTP("tcp", worker)
								if err != nil {
									log.Println("dialing:", err)
								}
								resp := &servers.MapRPCReply{}
								err = client.Call("WorkerServer.Map", &servers.MapRPCRequest{
									Key:   file,
									Value: data,
								}, resp)
								log.Printf("[DEBUG] %v map finished execution\n", worker)
								if err != nil {
									log.Printf("map [%v] exec: %v\n", worker, err)
								}
								mu.Lock()
								resultsList = append(resultsList, resp.KVA)
								mu.Unlock()
								log.Printf("[DEBUG] pushing %v results on to the chan\n", worker)
								client.Close()
							}(worker, chunks[i])
						}
						wg.Wait()
						//intermediateData := []servers.KeyValue{}
						//intermediateData = append(intermediateData, <-results...)

						log.Println("[DEBUG] Finished map")

						var writeMutex sync.Mutex
						shuffle := make(map[string][]string)
						for _, intermediateData := range resultsList {
							for _, kv := range intermediateData {
								shuffle[kv.Key] = append(shuffle[kv.Key], kv.Value)
							}
						}
						reduceResults := make(map[string]string)

						start = time.Now()

						i := 0
						//var wg sync.WaitGroup
						for key, value := range shuffle {
							wg.Add(1)
							go func(key string, values []string, worker string) {
								defer wg.Done()
								client, err := rpc.DialHTTP("tcp", worker)
								if err != nil {
									log.Println("dialing:", err)
								}
								resp := &servers.ReduceRPCReply{}
								err = client.Call("WorkerServer.Reduce", &servers.ReduceRPCRequest{
									Key:    key,
									Values: values,
								}, resp)
								//log.Printf("[DEBUG] %v reduce finished execution\n", worker)
								if err != nil {
									log.Printf("reduce [%v] exec: %v\n", worker, err)
								}

								writeMutex.Lock()
								//log.Printf("[DEBUG] Got results from %v: [%v]%v\n", worker, key, resp.Values)
								reduceResults[key] = resp.Values[0]
								writeMutex.Unlock()
								client.Close()

							}(key, value, serv.Workers[i%serv.MinWorkers])
							i++
						}
						wg.Wait()

						log.Println("[DEBUG] Finished reduce; Took: ", time.Since(start))

						start = time.Now()

						b, err = json.MarshalIndent(reduceResults, "", "\t")
						if err != nil {
							log.Fatalf("Encountered error during marshaling: %v", err)
						}

						f, err = os.Create(fmt.Sprintf("mapreduce-results.%d.out", fileIndex))
						if err != nil {
							log.Fatalf("Encountered error during file creation: %v", err)
						}

						_, err = f.Write(b)
						if err != nil {
							log.Fatalf("Encountered error during file writing: %v", err)
						}
						took = time.Since(start)

						f.Close()
						log.Print("[DEBUG] Finished writing; Took ", took)

						for _, worker := range serv.Workers {
							client, err := rpc.DialHTTP("tcp", worker)
							if err != nil {
								log.Println("dialing:", err)
							}
							client.Call("WorkerServer.Shutdown", nil, nil)
						}
					}
					completed = true
				}
				if completed {
					os.Exit(0)
				}
			}
		}(server)

		http.Serve(l, nil)
	}

}
