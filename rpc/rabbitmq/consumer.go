package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	Consume(ctx context.Context, rpcQueueName string, onMessage func(msg amqp.Delivery) ([]byte, error)) error
}

type baseConsumer struct {
	broker *amqp.Connection
	ch     *amqp.Channel
	prefix string
}

func NewConsumer(broker *amqp.Connection, prefix string) Consumer {
	return &baseConsumer{broker: broker, prefix: prefix}
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

func (b *baseConsumer) declareQueue(ch *amqp.Channel, queueName string) (amqp.Queue, error) {
	queue, err := ch.QueueDeclare(
		fmt.Sprintf("%s.rpc_queue", queueName), // name
		true,                                   // durable
		false,                                  // auto-delete
		false,                                  // exclusive
		false,                                  // no-wait
		amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": 5,
		}, // args
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return queue, nil
}

func (b *baseConsumer) Consume(ctx context.Context, rpcQueueName string, onMessage func(msg amqp.Delivery) ([]byte, error)) error {
	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	queue, err := b.declareQueue(ch, rpcQueueName)
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

	fmt.Println("[X] Consumer waiting for message. To exit press CTRL+C")

	go func() {
		for msg := range msgs {
			var payload interface{}
			json.Unmarshal(msg.Body, &payload)
			fmt.Printf("[X] Consumer received a message from %s queue with correlation id %s: %+v\n", msg.ReplyTo, msg.CorrelationId, payload)

			if res, err := onMessage(msg); err != nil {
				msg.Nack(false, true)
			} else {
				err = ch.PublishWithContext(
					ctx,
					"",          // exchange
					msg.ReplyTo, // routing key
					false,       // mandatory
					false,       // immediate
					amqp.Publishing{
						Body:          res,
						ContentType:   "application/json",
						CorrelationId: msg.CorrelationId,
					})
				if err != nil {
					msg.Nack(false, true)
				} else {
					msg.Ack(false)
				}
			}
		}
	}()

	return nil
}
