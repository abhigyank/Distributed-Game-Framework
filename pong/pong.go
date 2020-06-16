package pong

import (
	"github.com/veandco/go-sdl2/sdl"
	"github.com/segmentio/kafka-go"

	"../kafkaUtils"
	"../types"
	"context"
)

const winWidth, winHeight = 800, 600


func writeToKafka(keyState []uint8, kafkaWriter *kafka.Writer){

	if keyState[sdl.SCANCODE_UP] != 0 {
		playerPosition := "-10"
		kafkaUtils.PushKafkaMessage(context.Background(), kafkaWriter, nil, []byte(playerPosition))
	}

	if keyState[sdl.SCANCODE_DOWN] != 0 {
		playerPosition := "10"
		kafkaUtils.PushKafkaMessage(context.Background(), kafkaWriter, nil, []byte(playerPosition))
	}
}

func StartGame(kafkaWriter *kafka.Writer) {

	initEverything()
	defer sdl.Quit()

	window := createWindow("Pong - Game", winWidth, winHeight)
	defer window.Destroy()

	renderer := createRenderer(window)
	defer renderer.Destroy()

	texture := createTexture(renderer)
	defer texture.Destroy()

	white := types.Color{R: 255, G: 255, B: 255}
	pixels := make([]byte, winWidth*winHeight*4)

	player1 := types.Paddle{Position: types.Position{X: 50, Y: 100}, Width: 20, Height: 100, Color: white}
	player2 := types.Paddle{Position: types.Position{X: 750, Y: 100}, Width: 20, Height: 100, Color: white}
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

		writeToKafka(keyState, kafkaWriter)
		player1.Update(keyState)
		player2.AiUpdate(&ball)
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
