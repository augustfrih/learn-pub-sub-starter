package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack = iota
	NackReque
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("couldnt marshal json data error: %s\n", err)
	}

	amqpData := amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqpData)
	if err != nil {
		return fmt.Errorf("couldnt publish json. error: %s\n", err)
	}

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	chann, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	deliveries, err := chann.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for message := range deliveries {
			var data T
			err = json.Unmarshal(message.Body, &data)
			ackType := handler(data)
			switch ackType {
			case Ack:
				message.Ack(false)
				log.Printf("Message acked")
			case NackReque:
				message.Nack(false, true)
				log.Printf("Message nacked and requed")
			case NackDiscard:
				message.Nack(false, false)
				log.Printf("Message nacked and discarded")
			}
		}
	}()

	return nil
}
