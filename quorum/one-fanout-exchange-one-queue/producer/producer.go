package main

import (
	"context"
	"fmt"

	"go-rabbitmq/quorum/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Payload struct {
	AccountID string `json:"account_id"`
	Amount    uint32 `json:"amount"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := amqp.Dial("amqp://localhost")
	if err != nil {
		return
	}
	defer broker.Close()

	exchangeName := "account.transaction.withdraw.one.queue"
	producer := rabbitmq.NewProducer(broker, "dev")
	payload := &Payload{
		AccountID: "123",
		Amount:    500000,
	}
	err = producer.PublishFanout(ctx, exchangeName, payload)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("[X] Publish to %s exchange with message %+v", exchangeName, payload)
}
