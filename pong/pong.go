package pong

import (
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/veandco/go-sdl2/sdl"

	"context"

	"../kafkaUtils"
	"../types"
)

const winWidth, winHeight = 800, 600

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

func readServer(kafkaReaderServer *kafka.Reader) {
		m, err := kafkaReaderServer.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error while receiving message: %s\n", err.Error())
		}
		value := m.Value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))
}


func readOppositionPosition(kafkaReaderOpposition *kafka.Reader, player2 *types.Paddle, player1 *types.Paddle, firstPlayer bool) {
	m, err := kafkaReaderOpposition.ReadMessage(context.Background())
	if err != nil {
		fmt.Printf("error while receiving message: %s\n", err.Error())
	}

	value := m.Value
	fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))

	if firstPlayer {
		player2.UpdateFromDelta(string(value))
	} else {
		player1.UpdateFromDelta(string(value))
	}
}

func StartGame(firstPlayer bool, kafkaWriter *kafka.Writer, kafkaReaderServer *kafka.Reader, kafkaReaderOpposition *kafka.Reader) {
	initEverything()
	defer sdl.Quit()

	var playerID string
	if (firstPlayer) {
		playerID = "1"
	} else {
		playerID = "2"
	}

	window := createWindow("Pong - Game " + playerID, winWidth, winHeight)
	defer window.Destroy()

	renderer := createRenderer(window)
	defer renderer.Destroy()

	texture := createTexture(renderer)
	defer texture.Destroy()

	white := types.Color{R: 255, G: 255, B: 255}
	pixels := make([]byte, winWidth*winHeight*4)

	player1 := types.Paddle{Position: types.Position{X: 50, Y: 300}, Width: 20, Height: 100, Color: white}
	player2 := types.Paddle{Position: types.Position{X: 750, Y: 300}, Width: 20, Height: 100, Color: white}
	ball := types.Ball{Position: types.Position{X: 400, Y: 300}, Radius: 20, XVelocity: 10, YVelocity: 10, Color: white}
	running := true
	keyState := sdl.GetKeyboardState()

	for running {
		for event := sdl.PollEvent(); event != nil; event = sdl.PollEvent() {
			switch event.(type) {
			case *sdl.QuitEvent:
				running = false
				break
			}
		}

		clear(pixels)

		go readServer(kafkaReaderServer)

		go readOppositionPosition(kafkaReaderOpposition, &player2, &player1, firstPlayer)

		writeToKafka(keyState, kafkaWriter)


		if firstPlayer {
			player1.UpdateFromKeyState(keyState)
		} else {
			player2.UpdateFromKeyState(keyState)
		}

		ball.Update(&player1, &player2)

		player1.Draw(pixels)
		player2.Draw(pixels)
		ball.Draw(pixels)

		texture.Update(nil, pixels, winWidth*4)
		renderer.Copy(texture, nil, nil)
		renderer.Present()

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
