package types

import (
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
	X, Y float32
}

// Ball represents the ball of the game.
type Ball struct {
	Position
	Radius    int
	XVelocity float32
	YVelocity float32
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

// Update updates the ball position and controls collision.
func (ball *Ball) Update(leftPaddle *Paddle, rightPaddle *Paddle) {

	ball.Y += ball.YVelocity
	ball.X += ball.XVelocity

	if int(ball.Y) < 0+ball.Radius || int(ball.Y) > winHeight-ball.Radius {
		ball.YVelocity = -ball.YVelocity
	}

	if int(ball.X) < 0+ball.Radius || int(ball.X) > winWidth-ball.Radius {
		println("You lose!")
		os.Exit(0)
	}

	if ball.X < leftPaddle.X+float32(leftPaddle.Width/2)+float32(ball.Radius) {
		if ball.Y > leftPaddle.Y-float32(leftPaddle.Height/2)-float32(ball.Radius) && ball.Y < leftPaddle.Y+float32(leftPaddle.Height/2)+float32(ball.Radius) {
			ball.XVelocity = -ball.XVelocity
		}
	}

	if ball.X > rightPaddle.X-float32(rightPaddle.Width/2)-float32(ball.Radius) {
		if ball.Y > rightPaddle.Y-float32(rightPaddle.Height/2)-float32(ball.Radius) && ball.Y < rightPaddle.Y+float32(rightPaddle.Height/2)+float32(ball.Radius) {
			ball.XVelocity = -ball.XVelocity
		}
	}
}

// Draw draws the paddle.
func (paddle *Paddle) Draw(pixels []byte) {
	startX := int(paddle.X) - paddle.Width/2
	startY := int(paddle.Y) - paddle.Height/2

	for y := 0; y < paddle.Height; y++ {
		for x := 0; x < paddle.Width; x++ {
			setPixel(startX+x, startY+y, paddle.Color, pixels)
		}
	}
}

// Update updates the paddle position by checking the keyboard state.
func (paddle *Paddle) Update(keyState []uint8) {
	if keyState[sdl.SCANCODE_UP] != 0 {
		paddle.Y -= 10
	}

	if keyState[sdl.SCANCODE_DOWN] != 0 {
		paddle.Y += 10
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
