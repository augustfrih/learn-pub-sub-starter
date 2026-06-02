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
	const rabbitConnString = "amqp://guest:guest@127.0.0.1:5672/"
	rabbitConn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer rabbitConn.Close()

	fmt.Printf("Connection was succesful\n")

	gamelogic.PrintServerHelp()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	newChan, err := rabbitConn.Channel()
	if err != nil {
		log.Fatal(err)
		return
	}

InputLoop:
	for {
		input := gamelogic.GetInput()
		if err != nil {
			log.Fatalf("couldnt get input %v", err)
		}

		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message")
			playingState := routing.PlayingState{
				IsPaused: true,
			}

			err = pubsub.PublishJSON(newChan, routing.ExchangePerilDirect, routing.PauseKey, playingState)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}

		case "resume":
			fmt.Println("Sending resume message")
			playingState := routing.PlayingState{
				IsPaused: false,
			}

			err = pubsub.PublishJSON(newChan, routing.ExchangePerilDirect, routing.PauseKey, playingState)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}

		case "quit":
			fmt.Println("exiting program")
			return

		default:
			fmt.Println("cant understand command")

		}
	}

}
