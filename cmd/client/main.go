package main

import (
	"fmt"
	"log"

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

	gameState := gamelogic.NewGameState(userName)

	chann, err := rabbitConn.Channel()
	if err != nil {
		log.Printf("couldnt establish channel. error: %v", err)
	}

	err = pubsub.SubscribeJSON(
		rabbitConn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+userName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause, err: %v", err)
	}

	err = pubsub.SubscribeJSON(
		rabbitConn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+userName,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, chann),
	)
	if err != nil {
		log.Fatalf("could not subscribe to move, err: %v", err)
	}

	err = pubsub.SubscribeJSON(
		rabbitConn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gameState, chann),
	)


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
				log.Printf("couldnt move using command: %s. err: %v", input, err)
			} else {
				err = pubsub.PublishJSON(
					chann,
					routing.ExchangePerilTopic,
					routing.ArmyMovesPrefix+"."+userName,
					battleMove,
				)
				if err != nil {
					log.Printf("couldnt publish move using command: %s. err: %v", input, err)
				}
				if err == nil {
					log.Printf("move %s completed", battleMove.ToLocation)
				}
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
