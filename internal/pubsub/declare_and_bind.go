package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueTransient = iota
	SimpleQueueDurable
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	var table amqp.Table
	table = make(amqp.Table)
	table["x-dead-letter-exchange"] = "peril_dlx"

	chann, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := chann.QueueDeclare(queueName,
		queueType == SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		false,
		table)

	err = chann.QueueBind(queue.Name, key, exchange, false, nil)

	return chann, queue, nil
}
