package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	ConsumeFanout(_exchangeName string, onMessage func(msg amqp.Delivery) error) error
	ConsumeDirect(_exchangeName, routingKey string, onMessage func(msg amqp.Delivery) error) error
}

type baseConsumer struct {
	broker *amqp.Connection
	ch     *amqp.Channel
	group  string
	prefix string
}

func NewConsumer(broker *amqp.Connection, prefix, group string) Consumer {
	return &baseConsumer{broker: broker, prefix: prefix, group: group}
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

func (b *baseConsumer) declareExchange(ch *amqp.Channel, exchangeType string, exchangeName string) error {
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

func (b *baseConsumer) declareQueue(ch *amqp.Channel, exchangeName string) (amqp.Queue, error) {
	queue, err := ch.QueueDeclare(
		fmt.Sprintf("%s.%s.queue", exchangeName, b.group), // name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
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

func (b *baseConsumer) bindQueue(ch *amqp.Channel, queueName, exchangeName, routingKey string) error {
	err := ch.QueueBind(
		queueName,    // queue-name
		routingKey,   // routing-key
		exchangeName, // exchange-name
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return err
	}

	return nil
}

func (b *baseConsumer) ConsumeFanout(_exchangeName string, onMessage func(msg amqp.Delivery) error) error {
	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	exchangeName := fmt.Sprintf("%s.%s", b.prefix, _exchangeName)

	err = b.declareExchange(ch, "fanout", exchangeName)
	if err != nil {
		return err
	}
	queue, err := b.declareQueue(ch, exchangeName)
	if err != nil {
		return err
	}
	err = b.bindQueue(ch, queue.Name, exchangeName, "")
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
			// var payload interface{}
			// json.Unmarshal(msg.Body, &payload)
			// fmt.Printf("[X] Consumer received a message from %s exchange: %+v\n", msg.Exchange, payload)

			if err := onMessage(msg); err != nil {
				msg.Nack(false, true)
			} else {
				msg.Ack(false)
			}
		}
	}()

	return nil
}

func (b *baseConsumer) ConsumeDirect(_exchangeName, routingKey string, onMessage func(msg amqp.Delivery) error) error {
	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	exchangeName := fmt.Sprintf("%s.%s", b.prefix, _exchangeName)

	err = b.declareExchange(ch, "direct", exchangeName)
	if err != nil {
		return err
	}
	queue, err := b.declareQueue(ch, exchangeName)
	if err != nil {
		return err
	}
	err = b.bindQueue(ch, queue.Name, exchangeName, routingKey)
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
			// var payload interface{}
			// json.Unmarshal(msg.Body, &payload)
			// fmt.Printf("[X] Consumer received a message from %s exchange: %+v\n", msg.Exchange, payload)

			if err := onMessage(msg); err != nil {
				msg.Nack(false, true)
			} else {
				msg.Ack(false)
			}
		}
	}()

	return nil
}
