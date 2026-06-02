package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	const rabbitConnString = "amqp://guest:guest@127.0.0.1:5672/"
	rabbitConn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer rabbitConn.Close()

	fmt.Printf("Connection was succesful\n")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username, err: %v", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		rabbitConn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+userName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient)
	if err != nil {
		log.Fatalf("could not connect to pause, err: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)


	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Printf("Closing connection\n")
}
