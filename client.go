package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"./kafkaUtils"
	"./types"
	"github.com/segmentio/kafka-go"
)

func writePlayerPosition(writer *kafka.Writer) error {
	playerPosition := "player position"
	return kafkaUtils.PushKafkaMessage(context.Background(), writer, nil, []byte(playerPosition))
}

func game(client types.Client, kafka types.KafkaInfo, oppositeID string) {
	kafkaWriter := kafkaUtils.GetKafkaWriter([]string{kafka.Address + ":" + kafka.Port}, client.ID, client.ID)
	kafkaReaderServer := kafkaUtils.GetKafkaReader([]string{kafka.Address + ":" + kafka.Port}, client.ID, "server_0")
	defer kafkaReaderServer.Close()
	kafkaReaderOpposition := kafkaUtils.GetKafkaReader([]string{kafka.Address + ":" + kafka.Port}, client.ID, oppositeID)
	defer kafkaReaderOpposition.Close()

	fmt.Println("Got the kafkaReaders")

	for {
		m, err := kafkaReaderServer.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
			continue
		}

		value := m.Value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))

		writePlayerPosition(kafkaWriter)

		m, err = kafkaReaderOpposition.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
			continue
		}

		value = m.Value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))
	}
}

func main() {
	clientAddress := flag.String("clientAddress", "127.0.0.1:5000", "clientAddress in the format ip:port")
	serverAddress := flag.String("serverAddress", "127.0.0.1:3000", "serverAddress in the format ip:port")
	flag.Parse()

	conn, err := net.Dial("tcp", *serverAddress)
	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	request := *clientAddress + "\n"
	_, err = conn.Write([]byte(request))
	if err != nil {
		fmt.Println("Error writing to stream.")
		os.Exit(1)
	}

	fmt.Println("Written to stream.")

	kafkaBrokerURL, clientID, oppositeClientID := readConnection(conn)
	client := types.Client{clientID, strings.Split(*clientAddress, ":")[0], strings.Split(*clientAddress, ":")[1]}
	fmt.Println("KafkaServer: " + kafkaBrokerURL.Address + ":" + kafkaBrokerURL.Port)
	fmt.Println("ClientID: " + clientID + " OppositeClientID: " + oppositeClientID)
	game(client, kafkaBrokerURL, oppositeClientID)

}

func readConnection(conn net.Conn) (types.KafkaInfo, string, string) {
	for {
		scanner := bufio.NewScanner(conn)

		for {
			ok := scanner.Scan()
			response := scanner.Text()

			fmt.Println("Response: " + response)

			if !ok {
				fmt.Println("Reached EOF on server connection.")
				return types.KafkaInfo{}, "", ""
			}
			return types.KafkaInfo{strings.Split(response, ":")[0], strings.Split(response, ":")[1]}, strings.Split(response, ":")[2], strings.Split(response, ":")[3]
		}
	}
}
