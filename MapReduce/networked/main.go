package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			log.Fatal("listen error:", err)
		}
		addrSplit := strings.Split(l.Addr().String(), ":")
		clientServer.ListenAddr = getIPAddress() + ":" + addrSplit[len(addrSplit)-1]
		clientServer.CoordinatorServer = os.Args[2]

		go clientServer.HealthCheckRoutine()

		log.Println("Listening on", clientServer.ListenAddr)
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
						chunkSize := (len(data) + parts - 1) / parts
						offset := 0
						log.Printf("[DEBUG] Chunk size: %d", chunkSize)
						//fmt.Println(chunkSize)
						for i := 0; i < len(data); i += chunkSize {
							if i+chunkSize+offset > len(data) {
								log.Printf("[i=%v] size=%v chunkSize=%v parts=%v SPECIAL END CASE! slice[%v:%v]\n", i, len(data), chunkSize, parts, i, i+len(data)-1)
								chunks = append(chunks, data[i+offset:len(data)-1-offset])
							} else {
								if unicode.IsSpace(rune(data[i+offset+chunkSize])) {
									log.Printf("[i=%v] size=%v chunkSize=%v parts=%v [not space case]\n", i, len(data), chunkSize, parts)
									chunks = append(chunks, data[i+offset:i+chunkSize+offset])
								} else {
									for !unicode.IsSpace(rune(data[i+offset])) { // so that we dont cut off in the middle of a word (in text data)
										offset++
										log.Printf("Found case of not space: %q\n", data[i+offset])
									}
									log.Printf("[i=%v] size=%v chunkSize=%v parts=%v offset=%v\n", i, len(data), chunkSize, parts, offset)
									chunks = append(chunks, data[i+offset:i+chunkSize+offset])
								}

							}
						}

						resultsList := [][]servers.KeyValue{}
						var mu sync.Mutex
						var wg sync.WaitGroup
						mapFailures := make(chan servers.WorkerError)

						//results := make(chan []servers.KeyValue, len(serv.Workers))
						for i, worker := range serv.Workers {
							wg.Add(1)
							go servers.PreformMap(&wg, &mu, &resultsList, worker, i, file, chunks[i], mapFailures)
						}
						go func() {
							wg.Wait()
							close(mapFailures)
						}()

						for workErr := range mapFailures {
							wg.Add(1)
							// pick random worker to exec on
							serv.Deregister(&workErr.Worker)
							worker := serv.Workers[rand.Intn(len(serv.Workers))]
							go servers.PreformMap(&wg, &mu, &resultsList, worker, workErr.SelectedSlice, file, chunks[workErr.SelectedSlice], mapFailures)
						}

						log.Println("[DEBUG] Finished map")

						var writeMutex sync.Mutex
						shuffle := make(map[string][]string)
						for _, intermediateData := range resultsList {
							for _, kv := range intermediateData {
								shuffle[kv.Key] = append(shuffle[kv.Key], kv.Value)
							}
						}
						log.Println("[DEBUG] Finished shuffle")

						reduceResults := make(map[string]string)

						start = time.Now()

						i := 0
						guard := make(chan bool, len(serv.Workers)*10)
						// guard channel to make sure we dont go over the limit
						// also will only run at most 2 rpc requests on each worker at a time
						reduceFailures := make(chan servers.WorkerError)

						for key, value := range shuffle {
							wg.Add(1)
							guard <- true
							go servers.PreformReduce(&wg, &writeMutex, guard, key, value, reduceResults, serv.Workers[i%len(serv.Workers)], i, reduceFailures)
							i++
						}
						log.Println("[DEBUG] Finished submitting reduces")
						go func() {
							wg.Wait()
							close(reduceFailures)
						}()
						for workErr := range reduceFailures {
							wg.Add(1)
							// pick next worker to exec on
							worker := serv.Workers[(workErr.SelectedSlice+1)%len(serv.Workers)]
							serv.Deregister(&workErr.Worker)
							guard <- true
							log.Printf("[FailureRecovery] saving from %s by running on %s", workErr.Worker, worker)
							go servers.PreformReduce(&wg, &writeMutex, guard, workErr.Key, shuffle[workErr.Key], reduceResults, worker, workErr.SelectedSlice, reduceFailures)
						}

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

func getIPAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	// handle err...
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
