package pong

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/veandco/go-sdl2/sdl"

	"context"

	"../kafkaUtils"
	"../types"
)

const winWidth, winHeight = 800, 600
const numberOfBalls = 2

var kafkaDataArray [10000009]string
var maxItr = 0
var kafkaDataArrayBall [numberOfBalls][10000009]string
var maxItrBall[numberOfBalls]int // By default initialised with zero.
var maxLength = 10000009

func writeToKafka(keyState []uint8, kafkaWriter *kafka.Writer) {

	if keyState[sdl.SCANCODE_UP] != 0 {
		playerPosition := "-10"
		kafkaUtils.PushKafkaMessage(context.Background(), kafkaWriter, nil, []byte(playerPosition))
	}

	if keyState[sdl.SCANCODE_DOWN] != 0 {
		playerPosition := "10"
		kafkaUtils.PushKafkaMessage(context.Background(), kafkaWriter, nil, []byte(playerPosition))
	}

	// if keyState[sdl.SCANCODE_UP] == 0 && keyState[sdl.SCANCODE_DOWN] == 0 {
	// 	playerPosition := "0"
	// 	kafkaUtils.PushKafkaMessage(context.Background(), kafkaWriter, nil, []byte(playerPosition))
	// }
}

func readServer(kafkaReaderServer *kafka.Reader, ball *types.Ball, texture *sdl.Texture, renderer *sdl.Renderer, pixels []byte, ball_index int) {
	for {
		m, err := kafkaReaderServer.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
		}
		value := string(m.Value)
		kafkaDataArrayBall[ball_index][maxItrBall[ball_index]] = string(value)
		maxItrBall[ball_index] = (maxItrBall[ball_index] + 1) % maxLength
	}
}

func readOppositionPosition(kafkaReaderOpposition *kafka.Reader, player2 *types.Paddle, player1 *types.Paddle, firstPlayer bool, texture *sdl.Texture, renderer *sdl.Renderer, pixels []byte) {
	for {
		m, err := kafkaReaderOpposition.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
		}

		value := m.Value
		kafkaDataArray[maxItr] = string(value)
		maxItr = (maxItr + 1) % maxLength
	}
}

func renderGame(texture *sdl.Texture, renderer *sdl.Renderer, pixels []byte, firstPlayer bool, player2 *types.Paddle, player1 *types.Paddle, balls [numberOfBalls]*types.Ball) {
	playerArrayItr := 0
	var ballArrayItr [numberOfBalls]int

	for {
		if playerArrayItr != maxItr {
			if firstPlayer {
				player2.Clear(pixels)
				player2.UpdateFromDelta(kafkaDataArray[playerArrayItr])
				player2.Draw(pixels)
			} else {
				player1.Clear(pixels)
				player1.UpdateFromDelta(kafkaDataArray[playerArrayItr])
				player1.Draw(pixels)
			}
			playerArrayItr = (playerArrayItr + 1) % maxLength
		}

		for i:= 0; i < numberOfBalls; i++ {
			if ballArrayItr[i] != maxItrBall[i] {
				value := kafkaDataArrayBall[i][ballArrayItr[i]]
				ballPosition := strings.Split(value, ":")
				positionX, _ := strconv.ParseFloat(ballPosition[0], 32)
				positionY, _ := strconv.ParseFloat(ballPosition[1], 32)
				velocityX, _ := strconv.ParseFloat(ballPosition[2], 32)
				velocityY, _ := strconv.ParseFloat(ballPosition[3], 32)
	
				balls[i].Clear(pixels)
				balls[i].Set(positionX, positionY, velocityX, velocityY)
				balls[i].Draw(pixels)
				ballArrayItr[i] = (ballArrayItr[i] + 1) % maxLength
			}
		}

		texture.Update(nil, pixels, winWidth*4)
		renderer.Copy(texture, nil, nil)
		renderer.Present()
	}
}

// StartGame initializes the game
func StartGame(firstPlayer bool, kafkaWriter *kafka.Writer, kafkaReaderServer *kafka.Reader, kafkaReaderOpposition *kafka.Reader, kafkaBallReaders [numberOfBalls]*kafka.Reader) {
	initEverything()
	defer sdl.Quit()

	var playerID string
	if firstPlayer {
		playerID = "1"
	} else {
		playerID = "2"
	}

	window := createWindow("Pong - Game "+playerID, winWidth, winHeight)
	defer window.Destroy()

	renderer := createRenderer(window)
	defer renderer.Destroy()

	texture := createTexture(renderer)
	defer texture.Destroy()

	white := types.Color{R: 255, G: 255, B: 255}
	pixels := make([]byte, winWidth*winHeight*4)

	player1 := types.Paddle{Position: types.Position{X: 50, Y: 300}, Width: 20, Height: 100, Color: white}
	player2 := types.Paddle{Position: types.Position{X: 750, Y: 300}, Width: 20, Height: 100, Color: white}
	var balls [numberOfBalls]*types.Ball
	ball_0 := types.Ball{Position: types.Position{X: 400, Y: 300}, Radius: 20, XVelocity: 0.3, YVelocity: 0.3, Color: white}
	ball_1 := types.Ball{Position: types.Position{X: 400, Y: 300}, Radius: 20, XVelocity: -0.3, YVelocity: 0.3, Color: white}
	balls[0] = &ball_0
	balls[1] = &ball_1
	running := true
	keyState := sdl.GetKeyboardState()
	go readOppositionPosition(kafkaReaderOpposition, &player2, &player1, firstPlayer, texture, renderer, pixels)
	go renderGame(texture, renderer, pixels, firstPlayer, &player2, &player1, balls)
	for i, ball_element := range balls {
		go readServer(kafkaBallReaders[i], ball_element, texture, renderer, pixels, i)
		ball_element.Draw(pixels)
	} 
	player1.Draw(pixels)
	player2.Draw(pixels)
	for running {
		for event := sdl.PollEvent(); event != nil; event = sdl.PollEvent() {
			switch event.(type) {
			case *sdl.QuitEvent:
				running = false
				break
			}
		}

		writeToKafka(keyState, kafkaWriter)

		if firstPlayer {
			player1.Clear(pixels)
			player1.UpdateFromKeyState(keyState)
			player1.Draw(pixels)
		} else {
			player2.Clear(pixels)
			player2.UpdateFromKeyState(keyState)
			player2.Draw(pixels)
		}

		sdl.Delay(24)
	}
}

func clear(pixels []byte) {
	for i := range pixels {
		pixels[i] = 0
	}
}

func createTexture(renderer *sdl.Renderer) *sdl.Texture {
	texture, err := renderer.CreateTexture(sdl.PIXELFORMAT_ABGR8888, sdl.TEXTUREACCESS_STREAMING, winWidth, winHeight)
	if err != nil {
		panic(err)
	}
	return texture
}

func createRenderer(window *sdl.Window) *sdl.Renderer {
	renderer, err := sdl.CreateRenderer(window, -1, sdl.RENDERER_ACCELERATED)
	if err != nil {
		panic(err)
	}
	return renderer
}

func createWindow(title string, width, height int32) *sdl.Window {
	window, err := sdl.CreateWindow(title, sdl.WINDOWPOS_UNDEFINED, sdl.WINDOWPOS_UNDEFINED, width, height, sdl.WINDOW_SHOWN)
	if err != nil {
		panic(err)
	}
	return window
}

func initEverything() {
	if err := sdl.Init(sdl.INIT_EVERYTHING); err != nil {
		panic(err)
	}
}
