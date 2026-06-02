package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@127.0.0.1:5672/"
	rabbitConn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer rabbitConn.Close()

	fmt.Printf("Connection was succesful\n")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	newChan, err := rabbitConn.Channel()
	if err != nil {
		log.Fatal(err)
		return
	}
	playingState := routing.PlayingState{
		IsPaused: true,
	}

	err = pubsub.PublishJSON(newChan, routing.ExchangePerilDirect, routing.PauseKey, playingState)
	if err != nil {
		log.Fatal(err)
		return
	}
	<-signalChan
	fmt.Printf("Closing connection\n")
}
