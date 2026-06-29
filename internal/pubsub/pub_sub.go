package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

func UnmarshalJSON[T any](jsonData []byte) (T, error) {
	var data T
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return data, err
	}
	return data, nil
}

func UnmarshalGob[T any](data []byte) (T, error) {
	dat := bytes.NewBuffer(data)
	var tData T
	dec := gob.NewDecoder(dat)
	err := dec.Decode(&tData)
	if err != nil {
		return tData, err
	}
	return tData, nil
}

func SubscribeJSON [T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	err := Subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		UnmarshalJSON,
	)
	if err != nil {
		return err
	}
	return nil
}


func SubscribeGob [T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	err := Subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		UnmarshalGob,
	)
	if err != nil {
		return err
	}
	return nil
}

func Subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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
			data, err := unmarshaller(message.Body)
			if err != nil {
				log.Printf("error marshaling message, err: %s", err)
				return
			}
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(val); err != nil {
		return err
	}

	amqpData := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	}
	err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqpData)
	if err != nil {
		return err
	}

	return nil
}

func PublishGameLog(ch *amqp.Channel, userName, msg string) error {
	return PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+userName,
		routing.GameLog{
			Username: userName,
			CurrentTime: time.Now(),
			Message: msg,
		},
	)
}
