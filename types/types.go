package types

import (
	"fmt"
	"os"

	"github.com/veandco/go-sdl2/sdl"
)

// Client represents a player
type Client struct {
	ID      string
	Address string
	Port    string
}

// KafkaInfo represents the Kafka server information.
type KafkaInfo struct {
	Address string
	Port    string
}

const winWidth, winHeight = 800, 600

// Color represents the color of a given pixel.
type Color struct {
	R, G, B byte
}

// Position represents the position of a given element.
type Position struct {
	X, Y float64
}

// Ball represents the ball of the game.
type Ball struct {
	Position
	Radius    int
	XVelocity float64
	YVelocity float64
	Color     Color
}

// Paddle represents a player in the game.
type Paddle struct {
	Position
	Width  int
	Height int
	Color  Color
}

// Draw draws the ball.
func (ball *Ball) Draw(pixels []byte) {
	for y := -ball.Radius; y < ball.Radius; y++ {
		for x := -ball.Radius; x < ball.Radius; x++ {
			if x*x+y*y < ball.Radius*ball.Radius {
				setPixel(int(ball.X)+x, int(ball.Y)+y, ball.Color, pixels)
			}
		}
	}
}

// Clear clears the ball.
func (ball *Ball) Clear(pixels []byte) {
	for y := -ball.Radius; y < ball.Radius; y++ {
		for x := -ball.Radius; x < ball.Radius; x++ {
			if x*x+y*y < ball.Radius*ball.Radius {
				setPixel(int(ball.X)+x, int(ball.Y)+y, Color{0, 0, 0}, pixels)
			}
		}
	}
}

// Update updates the ball position and controls collision.
func (ball *Ball) Update(leftPaddle *Paddle, rightPaddle *Paddle) {

	ball.Y += ball.YVelocity
	ball.X += ball.XVelocity

	if int(ball.Y) < 0+ball.Radius || int(ball.Y) > winHeight-ball.Radius {
		ball.YVelocity = -ball.YVelocity
	}

	// if int(ball.X) < 0+ball.Radius || int(ball.X) > winWidth-ball.Radius {
	// 	println("You lose!")
	// 	os.Exit(0)
	// }

	if int(ball.X) > 850 {
		os.Exit(0)
	}

	if ball.X < leftPaddle.X+float64(leftPaddle.Width/2)+float64(ball.Radius) {
		fmt.Println("Gone left!")
		if ball.Y > leftPaddle.Y-float64(leftPaddle.Height/2)-float64(ball.Radius) && ball.Y < leftPaddle.Y+float64(leftPaddle.Height/2)+float64(ball.Radius) {
			fmt.Println("Bouncing")
			ball.XVelocity = -ball.XVelocity
		}
	}

	if ball.X > rightPaddle.X-float64(rightPaddle.Width/2)-float64(ball.Radius) {
		fmt.Println("Gone right!")
		if ball.Y > rightPaddle.Y-float64(rightPaddle.Height/2)-float64(ball.Radius) && ball.Y < rightPaddle.Y+float64(rightPaddle.Height/2)+float64(ball.Radius) {
			fmt.Println("Bouncing")
			ball.XVelocity = -ball.XVelocity
		}
	}
}

// Set updates the ball position.
func (ball *Ball) Set(XPosition, YPosition, XVelocity, YVelocity float64) {
	ball.X = XPosition
	ball.Y = YPosition
	ball.XVelocity, ball.YVelocity = XVelocity, YVelocity
}

// Draw draws the paddle.
func (paddle *Paddle) Draw(pixels []byte) {
	fmt.Println(paddle.Y)
	startX := int(paddle.X) - paddle.Width/2
	startY := int(paddle.Y) - paddle.Height/2

	for y := 0; y < paddle.Height; y++ {
		for x := 0; x < paddle.Width; x++ {
			setPixel(startX+x, startY+y, paddle.Color, pixels)
		}
	}
}

//Clear clears the paddle.
func (paddle *Paddle) Clear(pixels []byte) {
	startX := int(paddle.X) - paddle.Width/2
	startY := int(paddle.Y) - paddle.Height/2

	for y := 0; y < paddle.Height; y++ {
		for x := 0; x < paddle.Width; x++ {
			setPixel(startX+x, startY+y, Color{0, 0, 0}, pixels)
		}
	}
}

// UpdateFromKeyState updates the paddle position by checking the keyboard state.
func (paddle *Paddle) UpdateFromKeyState(keyState []uint8) {
	if keyState[sdl.SCANCODE_UP] != 0 {
		paddle.Y -= 3
	}

	if keyState[sdl.SCANCODE_DOWN] != 0 {
		paddle.Y += 3
	}
}

// UpdateFromDelta updates the paddle position by checking the delta.
func (paddle *Paddle) UpdateFromDelta(delta string) {
	if delta == "-10" {
		paddle.Y -= 3
	}

	if delta == "10" {
		paddle.Y += 3
	}
}

// AiUpdate updates the paddle position by using a simple AI.
func (paddle *Paddle) AiUpdate(ball *Ball) {
	paddle.Y = ball.Y
}

func setPixel(x, y int, c Color, pixels []byte) {
	index := (y*winWidth + x) * 4

	if index < len(pixels)-4 && index >= 0 {
		pixels[index] = c.R
		pixels[index+1] = c.G
		pixels[index+2] = c.B
	}
}
