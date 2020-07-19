# Distributed-Game-Framework

Distributing computation of different game elements in separate machines to remove computation load from the game rendering machine. 
All game element computations are done in the server in a distributed manner. Client machines connect to server to begin game, and then communicate with 
Kafka servers to get game element informations.

<img src="https://img.shields.io/badge/Go-v1.14-blue">

# Getting started

Distributed-Game-Framework requires Go >= 1.14 and a running Kafka  >= 2.12 server.

## Install Go Dependencies - 

* `go get -v -d -x github.com/veandco/go-sdl2/{sdl,img,mix,ttf}` <br>
* `go get -v -d -x github.com/segmentio/kafka-go`

## Running for developemnt -

1. Start kafka server prefereable on port `9092`.
2. Start server - `go run server.go --kafkaAddress localhost:9092`
3. In a new terminal start first client - `go run client.go --clientAddress 127.0.0.1:5000`
4. In another new terminal start second client - `go run client.go --clientAddress 127.0.0.1:5001`
5. Game starts once bootstrap is completed (5-7 seconds).
