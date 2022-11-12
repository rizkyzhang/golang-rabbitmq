package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	Consume(queueName string, onMessage func(msg amqp.Delivery) error) error
}

type baseConsumer struct {
	ctx    context.Context
	broker *amqp.Connection
	ch     *amqp.Channel
}

func NewConsumer(ctx context.Context, broker *amqp.Connection) Consumer {
	return &baseConsumer{ctx: ctx, broker: broker}
}

func (b *baseConsumer) getChannel() (*amqp.Channel, error) {
	if b.ch == nil || b.ch.IsClosed() {
		ch, err := b.broker.Channel()
		if err != nil {
			return nil, err
		}

		b.ch = ch
	}

	return b.ch, nil
}

func (b *baseConsumer) Consume(queueName string, onMessage func(msg amqp.Delivery) error) error {
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

	msgs, err := ch.Consume(
		queue.Name, // queue-name
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			if err := onMessage(msg); err != nil {
				fmt.Println(err.Error())
				msg.Nack(false, true)
			}

			msg.Ack(false)
		}
	}()

	return nil
}
