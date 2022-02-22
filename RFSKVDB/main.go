package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

var mu sync.Mutex
var database map[string]string
var replicasMu sync.Mutex
var replicas []string
var ipAddr string

func main() {
	ipAddr = getIPAddress()
	database = make(map[string]string)
	listener, err := net.Listen("tcp", "0.0.0.0:6789")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Listening on 6789 for DB")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
		}
		go handleTCPConn(conn)
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
func handleTCPConn(c net.Conn) {
	defer c.Close()
	input := bufio.NewScanner(c)
	fmt.Fprintf(c, "OK. REPLICAS: %v\n", replicas)
	for input.Scan() {
		// command sent (each line is a command)
		command := input.Text()
		fmt.Println(command)
		parts := strings.Fields(command)
		if len(parts) >= 1 {
			fmt.Println(parts)
			switch parts[0] {
			case "SET":
				if len(parts) < 3 {
					fmt.Fprintf(c, "BAD PARAMS\n")
					break
				}
				ok := AtomicSetDB(parts[1], parts[2])
				if ok {
					fmt.Fprintf(c, "OK\n")
				}
			case "GET":
				val, ok := AtomicReadDB(parts[1])
				if ok {
					fmt.Fprintf(c, "%s\n", val)
				} else {
					fmt.Fprintf(c, "NOT FOUND\n")
				}
			case "DEL":
				ok := AtomicDeleteDB(parts[1])
				if ok {
					fmt.Fprintf(c, "OK\n")
				} else {
					fmt.Fprintf(c, "NOT FOUND\n")
				}
			case "PING":
				fmt.Fprintf(c, "PONG\n")
			case "LEN":
				v, ok := AtomicLenDB(parts[1])
				if !ok {
					fmt.Fprintf(c, "NOT FOUND\n")
					break
				}
				fmt.Fprintf(c, "%v\n", v)
			case "APPEND":
				if len(parts) < 2 {
					fmt.Fprintf(c, "BAD PARAMS\n")
					break
				}
				v, ok := AtomicAppendDB(parts[1], parts[2])
				if !ok {
					fmt.Fprintf(c, "NOT FOUND\n")
					break
				}
				fmt.Fprintf(c, "%s\n", v)
			case "INC":
				// INC Key (amt)
				if len(parts) == 2 {
					v, ok := AtomicIncrementDB(parts[1], 1)
					if ok {
						fmt.Fprintf(c, "%s\n", v)
					} else {
						fmt.Fprintf(c, "NOT FOUND\n")
					}
				} else {
					amt, err := strconv.Atoi(parts[2])
					if err != nil {
						fmt.Fprintf(c, "BAD PARAMS\n")
						break
					}
					v, ok := AtomicIncrementDB(parts[1], amt)
					if ok {
						fmt.Fprintf(c, "%s\n", v)
					} else {
						fmt.Fprintf(c, "NOT FOUND\n")
					}
				}
			case "DEC":
				// INC Key (amt)
				if len(parts) == 2 {
					v, ok := AtomicDecrementDB(parts[1], 1)
					if ok {
						fmt.Fprintf(c, "%s\n", v)
					} else {
						fmt.Fprintf(c, "NOT FOUND\n")
					}
				} else {
					amt, err := strconv.Atoi(parts[2])
					if err != nil {
						fmt.Fprintf(c, "BAD PARAMS\n")
						break
					}
					v, ok := AtomicDecrementDB(parts[1], amt)
					if ok {
						fmt.Fprintf(c, "%s\n", v)
					} else {
						fmt.Fprintf(c, "NOT FOUND\n")
					}
				}
			case "JOIN":
				// join the primary's replication
				// secondary connects to primary, primary responds with results
				if len(parts) != 2 {
					fmt.Fprintf(c, "BAD PARAMS\n")
					break
				}
				JoinServer(parts[1], c)

			case "REGISTER":
				if len(parts) != 2 {
					fmt.Fprintf(c, "BAD PARAMS\n")
					break
				}
				replicasMu.Lock()
				replicas = append(replicas, parts[1])
				active := replicas
				replicasMu.Unlock()
				fmt.Fprintf(c, "%v\n", active)
			}
		}

	}
}

func JoinServer(server string, c io.Writer) {
	conn, err := net.Dial("tcp", server)
	if err != nil {
		fmt.Fprintf(c, "ERROR: %v\n", err)
		return
	}
	conn.Write([]byte("PING"))
	joinScan := bufio.NewScanner(conn)
	resp := joinScan.Text()
	fmt.Println(resp)
	if resp != "PONG" {
		fmt.Fprintf(c, "ERROR: not expected ping response\n")
		return
	}
	conn.Write([]byte(fmt.Sprintf("REGISTER %v", ipAddr)))
	resp = joinScan.Text()
	replicasMu.Lock()
	replicas = append(replicas, strings.Fields(resp)...)
	replicasMu.Unlock()

}

func AtomicSetDB(key, value string) bool {
	mu.Lock()
	defer mu.Unlock()
	database[key] = value
	return true
}

func AtomicReadDB(key string) (string, bool) {
	mu.Lock()
	defer mu.Unlock()
	value, ok := database[key]
	if !ok {
		return "", false
	}
	return value, true
}

func AtomicDeleteDB(key string) bool {
	mu.Lock()
	defer mu.Unlock()
	_, ok := database[key]
	if !ok {
		return false
	}
	delete(database, key)
	return true
}
func AtomicIncrementDB(key string, amt int) (string, bool) {
	mu.Lock()
	defer mu.Unlock()
	val, ok := database[key]
	if !ok {
		return "", false
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		return "", false
	}
	intVal += amt
	strVal := strconv.Itoa(intVal)
	database[key] = strVal
	return strVal, true
}
func AtomicDecrementDB(key string, amt int) (string, bool) {
	mu.Lock()
	defer mu.Unlock()
	val, ok := database[key]
	if !ok {
		return "", false
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		return "", false
	}
	intVal -= amt
	strVal := strconv.Itoa(intVal)
	database[key] = strVal
	return strVal, true
}
func AtomicAppendDB(key, value string) (string, bool) {
	mu.Lock()
	defer mu.Unlock()
	val, ok := database[key]
	if !ok {
		return "", false
	}
	database[key] = val + value
	return val + value, true
}
func AtomicLenDB(key string) (int, bool) {
	mu.Lock()
	defer mu.Unlock()
	value, ok := database[key]
	if !ok {
		return -1, false
	}
	return len(value), true
}
