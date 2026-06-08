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
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Fatalf("could not connect to pause, err: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gameState := gamelogic.NewGameState(userName)

	for {
		input := gamelogic.GetInput()
		if err != nil {
			log.Fatalf("couldnt get input %v", err)
		}

		switch input[0] {
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				log.Printf("couldnt spawn using command: %s", input)
			}

		case "move":
			battleMove, err := gameState.CommandMove(input)
			if err != nil {
				log.Printf("couldnt spawn using command: %s", input)
			} else {
				log.Printf("move %s completed", battleMove.ToLocation)
			}

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("cant understand command")

		}
	}
}
