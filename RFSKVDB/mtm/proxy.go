package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	listen := ":4444"
	fmt.Println("Listening on", listen)
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	upstream := ":5555"
	fmt.Println("Upstream on", upstream)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
		}
		go handleTCPConn(conn, upstream)
	}
}

func handleTCPConn(conn net.Conn, upstream string) {
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		log.Println(err)
	}
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := up.Read(buf)
			if err != nil {
				if err != io.EOF {
					break
				}
				log.Println(err)
			}
			fmt.Printf("4444 <- 5555: %s", buf[:n])
			conn.Write(buf[:n])
		}
	}()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err)
		}
		fmt.Printf("4444 -> 5555: %s", buf[:n])
		up.Write(buf[:n])
	}
}
