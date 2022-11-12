package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer interface {
	PublishFanout(ctx context.Context, _exchangeName string, _payload interface{}) error
	PublishDirect(ctx context.Context, _exchangeName string, routingKey string, _payload interface{}) error
}

type baseProducer struct {
	broker *amqp.Connection
	ch     *amqp.Channel
	prefix string
}

func NewProducer(broker *amqp.Connection, prefix string) Producer {
	return &baseProducer{broker: broker, prefix: prefix}
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

func (b *baseProducer) declareExchange(ch *amqp.Channel, exchangeType string, exchangeName string) error {
	err := ch.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-delete
		false,        // internal
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return err
	}

	return nil
}

func (b *baseProducer) PublishFanout(ctx context.Context, _exchangeName string, _payload interface{}) error {
	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	exchangeName := fmt.Sprintf("%s.%s", b.prefix, _exchangeName)
	err = b.declareExchange(ch, "fanout", exchangeName)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(_payload)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		ctx,
		exchangeName, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body:        payload,
			ContentType: "application/json",
		})
	if err != nil {
		return err
	}

	// fmt.Printf("[X] Publish to %s exchange with message %+v\n", exchangeName, _payload)

	return nil
}

func (b *baseProducer) PublishDirect(ctx context.Context, _exchangeName string, routingKey string, _payload interface{}) error {
	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	exchangeName := fmt.Sprintf("%s.%s", b.prefix, _exchangeName)

	payload, err := json.Marshal(_payload)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		ctx,
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body:        payload,
			ContentType: "application/json",
		})
	if err != nil {
		return err
	}

	// fmt.Printf("[X] Publish to %s exchange with message %+v\n", exchangeName, _payload)

	return nil
}
