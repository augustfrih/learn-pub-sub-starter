package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("couldnt marshal json data error: %s\n", err)
	}

	amqpData := amqp.Publishing{
		ContentType: "application/json",
		Body: data,
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqpData)
	if err != nil {
		return fmt.Errorf("couldnt publish json. error: %s\n", err)
	}

	return nil
}
