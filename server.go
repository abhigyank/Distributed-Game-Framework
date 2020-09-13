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
	"sync"
	"time"

	"./kafkaUtils"
	"./types"
	"github.com/segmentio/kafka-go"
)

const numberOfBalls = 2

func readPlayerPosition(kafkaReader *kafka.Reader, player *types.Paddle) {
	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
		}

		value := m.Value

		player.UpdateFromDelta(string(value))
	}
}

func game(client1 types.Client, client2 types.Client, kafkaInfo types.KafkaInfo) {
	kafkaWriter := kafkaUtils.GetKafkaWriterBall([]string{kafkaInfo.Address + ":" + kafkaInfo.Port}, "server", "server_0")

	var kafkaBallWriters [numberOfBalls]*kafka.Writer
	for i := 0; i < numberOfBalls; i++ {
		kafkaUtils.CreateTopic(kafkaInfo.Address+":"+kafkaInfo.Port, "ball_"+strconv.Itoa(i)+"_0")
		kafkaBallWriters[i] = kafkaUtils.GetKafkaWriterBall([]string{kafkaInfo.Address + ":" + kafkaInfo.Port}, "server", "ball_"+strconv.Itoa(i)+"_0")
	}

	kafkaReaderClient1 := kafkaUtils.GetKafkaReader([]string{kafkaInfo.Address + ":" + kafkaInfo.Port}, "server", client1.ID+"_0")
	defer kafkaReaderClient1.Close()

	kafkaReaderClient2 := kafkaUtils.GetKafkaReader([]string{kafkaInfo.Address + ":" + kafkaInfo.Port}, "server", client2.ID+"_0")
	defer kafkaReaderClient1.Close()

	err := writeToStartGame(kafkaWriter)
	if err != nil {
		fmt.Println("Error occured while writing to start game", err)
	}

	// Wait for both servers to bootstrap, ideally we should wait for acknoledgement from both that they are ready to start.
	time.Sleep(10 * time.Second)

	white := types.Color{R: 255, G: 255, B: 255}
	player1 := types.Paddle{Position: types.Position{X: 50, Y: 300}, Width: 20, Height: 100, Color: white}
	player2 := types.Paddle{Position: types.Position{X: 750, Y: 300}, Width: 20, Height: 100, Color: white}
	ball0 := types.Ball{Position: types.Position{X: 400, Y: 325}, Radius: 20, XVelocity: -1.0, YVelocity: -1.0, Color: white}
	ball1 := types.Ball{Position: types.Position{X: 400, Y: 275}, Radius: 20, XVelocity: 1.0, YVelocity: 1.0, Color: white}
	var balls [numberOfBalls]*types.Ball
	balls[0] = &ball0
	balls[1] = &ball1

	go readPlayerPosition(kafkaReaderClient1, &player1)
	go readPlayerPosition(kafkaReaderClient2, &player2)
	for {
		var wg sync.WaitGroup
		wg.Add(numberOfBalls)
		for i := 0; i < numberOfBalls; i++ {
			go func(i int) {
				defer wg.Done()
				writeBallPosition(kafkaBallWriters[i], balls[i])
				balls[i].Update(&player1, &player2)
			}(i)
		}
		wg.Wait()
		types.BallCollision(balls[:])
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
