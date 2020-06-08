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
)

func main() {
	clientID := flag.String("clientID", "1", "int: clientID")
	clientAddress := flag.String("clientAddress", "127.0.0.1:5000", "clientAddress in the format ip:port")
	serverAddress := flag.String("serverAddress", "127.0.0.1:3000", "serverAddress in the format ip:port")
	flag.Parse()

	client := types.Client{*clientID, strings.Split(*clientAddress, ":")[0], strings.Split(*clientAddress, ":")[1]}

	conn, err := net.Dial("tcp", *serverAddress)
	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	request := client.ID + ":" + client.Address + ":" + client.Port + "\n"
	_, err = conn.Write([]byte(request))
	if err != nil {
		fmt.Println("Error writing to stream.")
		os.Exit(1)
	}

	fmt.Println("Written to stream.")

	kafkaBrokerURL := readConnection(conn)
	fmt.Println("KafkaServer: " + kafkaBrokerURL.Address + ":" + kafkaBrokerURL.Port)

	kafkaReader := types.GetKafkaReader([]string{kafkaBrokerURL.Address + ":" + kafkaBrokerURL.Port}, client.ID, "server_0")
	defer kafkaReader.Close()

	fmt.Println("Got the kafkaReader")

	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
			continue
		}

		value := m.Value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))
	}

}

func readConnection(conn net.Conn) types.KafkaInfo {
	for {
		scanner := bufio.NewScanner(conn)

		for {
			ok := scanner.Scan()
			response := scanner.Text()

			fmt.Println("Response: " + response)

			if !ok {
				fmt.Println("Reached EOF on server connection.")
				return types.KafkaInfo{}
			}
			return types.KafkaInfo{strings.Split(response, ":")[0], strings.Split(response, ":")[1]}
		}
	}
}
