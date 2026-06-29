package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := pubsub.PublishGameLog(
				ch,
				gs.GetUsername(),
				fmt.Sprintf("%s won a war against %s", winner, loser),
			)
			if err != nil {
				log.Printf("failed to publish game log error: %s", err)
				return pubsub.NackReque
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := pubsub.PublishGameLog(
				ch,
				gs.GetUsername(),
				fmt.Sprintf("%s won a war against %s", winner, loser),
			)
			if err != nil {
				log.Printf("failed to publish game log error: %s", err)
				return pubsub.NackReque
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := pubsub.PublishGameLog(
				ch,
				gs.GetUsername(),
				fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
			)
			if err != nil {
				log.Printf("failed to publish game log error: %s", err)
				return pubsub.NackReque
			}
			return pubsub.Ack
		default:
			log.Printf("weird outcome from handlewar: %v", outcome)
			return pubsub.NackDiscard

		}
	}
}
