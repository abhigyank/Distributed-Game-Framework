package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"./types"
	"github.com/segmentio/kafka-go"
)

func createServer(serverPort string, kafka types.KafkaInfo) {
	var client1, client2 types.Client
	ln, err := net.Listen("tcp", fmt.Sprint(":"+serverPort))
	if err != nil {
		fmt.Println("Error occured in creating server: ", err)
		os.Exit(0)
	}
	for (client1 == types.Client{} || client2 == types.Client{}) {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error occured in accepting connection: ", err)
		} else {
			fmt.Println("Received connection a client.")
			message, _ := bufio.NewReader(conn).ReadString('\n')
			request := string(message)
			fmt.Print("Request: ", request)
			// Update client1 info and then client2 info
			client := types.Client{strings.Split(request, ":")[0], strings.Split(request, ":")[1], strings.Split(request, ":")[2]}
			if client1.ID != "" {
				client1 = client
			} else if client2.ID != "" {
				client2 = client
			}
			// Send kafka information to the clients
			response := kafka.Address + ":" + kafka.Port + "\n"
			conn.Write([]byte(response))
			conn.Close()
		}
	}
}

func writeBallPosition(writer *kafka.Writer) error {
	ballPosition := "ball position"
	return types.PushKafkaMessage(context.Background(), writer, nil, []byte(ballPosition))
}

func main() {
	kafkaAddress := flag.String("kafkaAddress", "127.0.0.1:8080", "kafkaAddress in  format ip:port")
	flag.Parse()

	kafka := types.KafkaInfo{strings.Split(*kafkaAddress, ":")[0], strings.Split(*kafkaAddress, ":")[1]}
	serverPort := "3000"

	kafkaWriter := types.GetKafkaWriter([]string{kafka.Address + ":" + kafka.Port}, "server", "server")

	fmt.Println("Creating server node...")
	defer createServer(serverPort, kafka)
	err := writeBallPosition(kafkaWriter)
	if err != nil {
		fmt.Println("Error occured while writing to stream", err)
	}
}
