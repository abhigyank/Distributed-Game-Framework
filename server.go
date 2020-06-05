package main

import (
	"fmt"
	"net"
	"os"
	"flag"
	"strings"
)

	
type client struct {
    id string
    address string
    port string
}

type kafkaInfo struct {
	address string
	port string
}

func createServer(serverPort string, kafka kafkaInfo) {
	var client1, client2 client;
	ln, err := net.Listen("tcp", fmt.Sprint(":" + serverPort))
	if err != nil {
		fmt.Println("Error occured in creating server: ", err)
		os.Exit(0)
	}
	for (client1 == client{} || client2 == client{}) {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error occured in accepting connection: ", err)
		} else {
			fmt.Println("Received connection a client.")
			// Update client1 info and then client2 info
			conn.Close()
		}
	}
	// Send kafka information to the clients

}

func main() {
	kafkaAddress := flag.String("kafkaAddress", "127.0.0.1:8080", "kafkaAddress in  format ip:port")
	flag.Parse()

	kafka := kafkaInfo{strings.Split(*kafkaAddress, ":")[0], strings.Split(*kafkaAddress, ":")[1]}
	serverPort := "3000";

	fmt.Println("Creating server node...")
	createServer(serverPort, kafka);
}