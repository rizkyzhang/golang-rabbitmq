package rabbitmq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer interface {
	Publish(queueName string, _payload interface{}) error
}

type baseProducer struct {
	ctx    context.Context
	broker *amqp.Connection
	ch     *amqp.Channel
}

func NewProducer(ctx context.Context, broker *amqp.Connection) Producer {
	return &baseProducer{ctx: ctx, broker: broker}
}

func (b *baseProducer) getChannel() (*amqp.Channel, error) {
	if b.ch == nil || b.ch.IsClosed() {
		ch, err := b.broker.Channel()
		if err != nil {
			return nil, err
		}

		b.ch = ch
	}

	return b.ch, nil
}

func (b *baseProducer) Publish(queueName string, _payload interface{}) error {
	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // auto-delete
		false,     // exclusive
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(_payload)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		b.ctx,
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Body:         payload,
			ContentType:  "application/json",
			DeliveryMode: 2,
		})
	if err != nil {
		return err
	}

	return nil
}
