package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"./kafkaUtils"
	"./types"
	"github.com/segmentio/kafka-go"
)

func readPlayerPosition(kafkaReader *kafka.Reader, player *types.Paddle, ball *types.Ball) {
	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
		}

		value := m.Value
		// fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))

		player.UpdateFromDelta(string(value))
	}
}

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

	white := types.Color{R: 255, G: 255, B: 255}
	player1 := types.Paddle{Position: types.Position{X: 50, Y: 300}, Width: 20, Height: 100, Color: white}
	player2 := types.Paddle{Position: types.Position{X: 750, Y: 300}, Width: 20, Height: 100, Color: white}
	ball := types.Ball{Position: types.Position{X: 400, Y: 300}, Radius: 20, XVelocity: .2, YVelocity: .2, Color: white}

	go readPlayerPosition(kafkaReaderClient1, &player1, &ball)
	go readPlayerPosition(kafkaReaderClient2, &player2, &ball)
	for {

		writeBallPosition(kafkaWriter, &ball)
		ball.Update(&player1, &player2)
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

func writeBallPosition(writer *kafka.Writer, ball *types.Ball) {
	positionX, positionY := strconv.FormatFloat(ball.X, 'f', -1, 32), strconv.FormatFloat(ball.Y, 'f', -1, 32)
	velocityX, velocityY := strconv.FormatFloat(ball.XVelocity, 'f', -1, 32), strconv.FormatFloat(ball.YVelocity, 'f', -1, 32)
	ballPosition := positionX + ":" + positionY + ":" + velocityX + ":" + velocityY
	err := kafkaUtils.PushKafkaMessage(context.Background(), writer, nil, []byte(ballPosition))
	if err != nil {
		fmt.Println("Error occured while writing to stream", err)
	} else {
		// fmt.Println("Ball position written")
	}
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
