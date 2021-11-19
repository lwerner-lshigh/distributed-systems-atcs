package servers

import (
	"errors"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

type PingResponse struct {
	Message string
}

type CoordinatorServer struct {
	workersMutex sync.Mutex
	Workers      []string
	Files        []string
	MinWorkers   int
	//HTTPWorkers  map[string]*rpc.Client
}

func (s *CoordinatorServer) Register(addr *string, reply *bool) error {
	s.workersMutex.Lock()
	s.Workers = append(s.Workers, *addr)
	log.Printf("[DEBUG] Registering %v worker\n", *addr)
	//client, err := rpc.DialHTTP("tcp", *addr)
	//if err != nil {
	//  log.Println("register dialing:", err)
	//	return err
	//}
	//s.HTTPWorkers[*addr] = client
	s.workersMutex.Unlock()
	return nil
}

func (s *CoordinatorServer) Deregister(addr *string) error {
	s.workersMutex.Lock()
	log.Printf("[DEBUG] Deregistering %v worker\n", *addr)
	i := indexOf(*addr, s.Workers)
	log.Printf("[DEBUG] Index of %v is %v\n", *addr, i)
	if i == -1 {
		return errors.New("worker not found")
	}
	s.Workers = remove(s.Workers, i)
	log.Printf("[DEBUG] Workers after deregistration: %v\n", s.Workers)
	//delete(s.HTTPWorkers, *addr)
	s.workersMutex.Unlock()
	return nil
}

func (s *CoordinatorServer) GetWorker(i int) string {
	s.workersMutex.Lock()
	val := s.Workers[i]
	s.workersMutex.Unlock()
	return val
}

func (s *CoordinatorServer) HealthCheckRoutine() {
	for {
		time.Sleep(time.Second * 1)
		for _, worker := range s.Workers {
			func(worker string) {
				// recover from panic
				defer func() {
					// make sure that we are not crashing ever
					if r := recover(); r != nil {
						log.Println("[ERROR] Coordinator server health check routine failed:", r)
					}
				}()
				// Make sure our friend is still alive
				client, err := rpc.DialHTTP("tcp", worker)
				if err != nil {
					// oh no they are dead! time to forget them
					log.Println("[HEALTH] dialing:", err)
					s.Deregister(&worker)
					return
				}
				defer client.Close()
				resp := &PingResponse{}
				err = client.Call("WorkerServer.Ping", "PING", resp)
				// Now say Hi!
				if err != nil {
					// what?! they are dead now?! time to forget them here too
					log.Println("[HEALTH] pinging:", err)
					s.Deregister(&worker)
					return
				}

			}(worker)
		}
	}
}

func (s *CoordinatorServer) Ping(req string, resp *PingResponse) error {
	resp.Message = "pong"
	return nil
}

// TODO: add register data function to allow a user to send jobs to the server without needing to restart the coordinator

type WorkerServer struct {
	CoordinatorServer string
	ListenAddr        string
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

func (w *WorkerServer) Ping(req string, resp *PingResponse) error {
	resp.Message = "pong"
	return nil
}

func (w *WorkerServer) HealthCheckRoutine() {
	notHealthy := true
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("[ERROR] Worker server health check routine failed:", r)
				}
			}()
			client, err := rpc.DialHTTP("tcp", w.CoordinatorServer)
			if err != nil {
				//log.Println("dialing:", err)
				notHealthy = true
				return
			}
			defer client.Close()
			if notHealthy {
				err = client.Call("CoordinatorServer.Register", &w.ListenAddr, nil)
				if err != nil {
					log.Println("registering:", err)
					return
				}
				notHealthy = false
				log.Printf("[DEBUG] Worker %v registered with coordinator %s\n", w.ListenAddr, w.CoordinatorServer)
			}
			time.Sleep(time.Second * 2)
		}()
	}
}

func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}
