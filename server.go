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

	"./types"
	"github.com/segmentio/kafka-go"
)

var TIME_TO_FOR_TOPIC_CREATE time.Duration = 1
var TIME_TO_FOR_TOPIC_DELETE time.Duration = 1

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

func deleteTopics(kafkaAddress string) {
	conn, _ := kafka.Dial("tcp", kafkaAddress)
	partitions, _:= conn.ReadPartitions()
	for i :=0; i < len(partitions); i++ {
		if (strings.Contains(partitions[i].Topic, "_0")) {
			fmt.Println("Deleting topic:", partitions[i].Topic)
			deleteTopic(kafkaAddress, partitions[i].Topic)			
		}
	}

}

func deleteTopic(kafkaAddress string, topicName string) {
	conn, _ := kafka.Dial("tcp", kafkaAddress)

	err := conn.DeleteTopics(topicName)
	if(err != nil){
		// Can occur when topic doesn't exists
		fmt.Println("Deletion error for topic", topicName, err)
	} else {
		//Wait for deletion to complete.
		time.Sleep(TIME_TO_FOR_TOPIC_DELETE * time.Second)
	}

	conn.Close()
}

func createTopic(kafkaAddress string, topicName string) {
	conn, _ := kafka.Dial("tcp", kafkaAddress)
	topic := kafka.TopicConfig{
		Topic:        topicName,
		NumPartitions: 1,
		ReplicationFactor: 1,
	}
	err := conn.CreateTopics(topic)
	if(err != nil){
		fmt.Println("Creation error for topic", topicName, err)
	} else {
		//Wait for creation to complete.
		time.Sleep(TIME_TO_FOR_TOPIC_CREATE * time.Second)
	}

	conn.Close()
}

func main() {
	kafkaAddress := flag.String("kafkaAddress", "127.0.0.1:8080", "kafkaAddress in  format ip:port")
	flag.Parse()

	kafka := types.KafkaInfo{strings.Split(*kafkaAddress, ":")[0], strings.Split(*kafkaAddress, ":")[1]}
	serverPort := "3000"

	fmt.Println("Deleting exiting kafka topics...")
	deleteTopics(*kafkaAddress)

	fmt.Println("Creating server_0 kafka topic...")
	createTopic(*kafkaAddress, "server_0")

	kafkaWriter := types.GetKafkaWriter([]string{kafka.Address + ":" + kafka.Port}, "server", "server_0")

	fmt.Println("Creating server node...")
	defer createServer(serverPort, kafka)
	err := writeBallPosition(kafkaWriter)
	if err != nil {
		fmt.Println("Error occured while writing to stream", err)
	}
	fmt.Println("Ball position written")

}
