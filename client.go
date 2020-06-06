package main

import (
	"bufio"
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

	kafka := readConnection(conn)
	fmt.Println("KafkaServer: " + kafka.Address + ":" + kafka.Port)
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
