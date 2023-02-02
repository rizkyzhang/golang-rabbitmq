package main

import (
	"context"
	"fmt"

	"go-rabbitmq/rpc/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Payload struct {
	Input string `json:"input"`
}

type ResponseData struct {
	Answer int `json:"answer"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := amqp.Dial("amqp://localhost")
	if err != nil {
		return
	}
	defer broker.Close()

	rpcQueueName := "rpc.test"
	producer := rabbitmq.NewProducer(broker, "dev")
	payload := &Payload{
		Input: "1 + 2",
	}

	_res := producer.Publish(ctx, rpcQueueName, payload)
	res, _ := rabbitmq.ParseRPCResponseData[ResponseData](_res)
	fmt.Printf("[x] Parsed response data: %+v", res)
}
