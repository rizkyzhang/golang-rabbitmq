package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	model "go-rabbitmq/rpc"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer interface {
	Publish(ctx context.Context, rpcQueueName string, _payload interface{}) *model.RPCResponse
}

type baseProducer struct {
	broker *amqp.Connection
	prefix string
}

func NewProducer(broker *amqp.Connection, prefix string) Producer {
	return &baseProducer{broker: broker, prefix: prefix}
}

func ParseRPCResponseData[T interface{}](res *model.RPCResponse) (*T, error) {
	if res.Status != "success" {
		return nil, errors.New(res.Error)
	}

	var resData T
	err := json.Unmarshal(res.Data, &resData)
	if err != nil {
		return nil, err
	}

	return &resData, nil
}

func getCorrelationID() string {
	return uuid.NewString()
}

func getFailedRPCResponse(err error) *model.RPCResponse {
	return &model.RPCResponse{
		Status: "failed",
		Error:  err.Error(),
	}
}

func (b *baseProducer) declareQueue(ch *amqp.Channel) (amqp.Queue, error) {
	queue, err := ch.QueueDeclare(
		"",           // name
		false,        // durable
		true,         // auto-delete
		true,         // exclusive
		false,        // no-wait
		amqp.Table{}, // args
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return queue, nil
}

func (b *baseProducer) Publish(ctx context.Context, rpcQueueName string, _payload interface{}) *model.RPCResponse {
	correlationID := getCorrelationID()
	ch, err := b.broker.Channel()
	if err != nil {
		return getFailedRPCResponse(err)
	}
	defer ch.Close()

	queue, err := b.declareQueue(ch)
	if err != nil {
		return getFailedRPCResponse(err)
	}

	payload, err := json.Marshal(_payload)
	if err != nil {
		return getFailedRPCResponse(err)
	}

	err = ch.PublishWithContext(
		ctx,
		"", // exchange
		fmt.Sprintf("%s.rpc_queue", rpcQueueName), // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Body:          payload,
			ContentType:   "application/json",
			CorrelationId: correlationID,
			ReplyTo:       queue.Name,
		})
	if err != nil {
		return getFailedRPCResponse(err)
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
		return getFailedRPCResponse(err)
	}

	resChan := make(chan *model.RPCResponse)

	go func() {
		for msg := range msgs {
			if correlationID == msg.CorrelationId {
				var res model.RPCResponse
				err := json.Unmarshal(msg.Body, &res)
				if err != nil {
					resChan <- getFailedRPCResponse(err)
					break
				}

				fmt.Printf("[x] Received response: %+v\n", res)
				resChan <- &res
			}
		}
	}()

	fmt.Printf("[X] Publish to %s queue with correlation id %s: %+v\n", rpcQueueName, correlationID, _payload)

	return <-resChan
}
