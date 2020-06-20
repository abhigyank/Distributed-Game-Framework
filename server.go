package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"./kafkaUtils"
	"./types"
	"github.com/segmentio/kafka-go"
)

func game(client1 types.Client, client2 types.Client, kafka types.KafkaInfo) {
	kafkaWriter := kafkaUtils.GetKafkaWriter([]string{kafka.Address + ":" + kafka.Port}, "server", "server_0")

	kafkaReaderClient1 := kafkaUtils.GetKafkaReader([]string{kafka.Address + ":" + kafka.Port}, "server", client1.ID+"_0")
	defer kafkaReaderClient1.Close()

	kafkaReaderClient2 := kafkaUtils.GetKafkaReader([]string{kafka.Address + ":" + kafka.Port}, "server", client2.ID+"_0")
	defer kafkaReaderClient1.Close()

	// Wait for both servers to bootstrap, ideally we should wait for acknoledgement from both that they are ready to start.
	time.Sleep(5 * time.Second)

	err := writeToStartGame(kafkaWriter)
	if err != nil {
		fmt.Println("Error occured while writing to start game", err)
	}

	for {
		err := writeBallPosition(kafkaWriter)
		if err != nil {
			fmt.Println("Error occured while writing to stream", err)
		}
		fmt.Println("Ball position written")

		m, err := kafkaReaderClient1.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
			continue
		}

		value := m.Value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))

		m, err = kafkaReaderClient2.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
			continue
		}

		value = m.Value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))
	}
}

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
			var currentID, otherID string
			if client1.ID == "" {
				fmt.Println("Player 1 connected.")
				currentID, otherID = "1", "2"
				client1 = types.Client{currentID, strings.Split(request, ":")[0], strings.Split(request, ":")[1]}
			} else if client2.ID == "" {
				fmt.Println("Player 2 connected.")
				currentID, otherID = "2", "1"
				client2 = types.Client{currentID, strings.Split(request, ":")[0], strings.Split(request, ":")[1]}
			}
			// Send kafka information to the clients
			response := kafka.Address + ":" + kafka.Port + ":" + currentID + ":" + otherID + "\n"
			conn.Write([]byte(response))
			if client1.ID != "" && client2.ID != "" {
				fmt.Println("Both players connected, commencing game.")
				game(client1, client2, kafka)
			}
			conn.Close()
		}
	}
}

func writeToStartGame(writer *kafka.Writer) error {
	startGame := "Start Game!"
	return kafkaUtils.PushKafkaMessage(context.Background(), writer, nil, []byte(startGame))
}

func writeBallPosition(writer *kafka.Writer) error {
	ballPosition := "ball position"
	return kafkaUtils.PushKafkaMessage(context.Background(), writer, nil, []byte(ballPosition))
}

func main() {
	kafkaAddress := flag.String("kafkaAddress", "127.0.0.1:8080", "kafkaAddress in  format ip:port")
	flag.Parse()

	kafka := types.KafkaInfo{strings.Split(*kafkaAddress, ":")[0], strings.Split(*kafkaAddress, ":")[1]}
	serverPort := "3000"

	fmt.Println("Deleting exiting kafka topics...")
	kafkaUtils.DeleteTopics(*kafkaAddress)

	fmt.Println("Creating server_0 kafka topic...")
	kafkaUtils.CreateTopic(*kafkaAddress, "server_0")

	fmt.Println("Creating server node...")
	createServer(serverPort, kafka)

}
